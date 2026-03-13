const axios = require('axios')
const fs = require('fs')
const path = require('path')
const stream = require('stream')
const { google } = require('googleapis')

// --- CONFIGURATION ---
const API_KEY =
  process.env.DATA_GOV_API_KEY ||
  '579b464db66ec23bdd000001e8d75b7cb11147365a5647630c56832b'
const RESOURCE_ID = '35985678-0d79-46b4-9ed6-6f13308a1d24'
const LIMIT = 5000
const CHUNK_SIZE = 500000
const FOLDER_ID = '1TNYEd-5CCzypE-mYfSsBH7yzr9iH7_Z2'
const NUM_WORKERS = 4

// Railway Volume Path (Default to current dir if not set)
const STORAGE_DIR = process.env.RAILWAY_VOLUME_MOUNT_PATH || './data'

// Ensure storage directory exists
if (!fs.existsSync(STORAGE_DIR)) {
  fs.mkdirSync(STORAGE_DIR, { recursive: true })
}

const OFFSET_FILE = path.join(STORAGE_DIR, 'offset.txt')
const PART_FILE = path.join(STORAGE_DIR, 'part.txt')

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms))
}

function getOffset() {
  if (fs.existsSync(OFFSET_FILE)) {
    return parseInt(fs.readFileSync(OFFSET_FILE, 'utf8').trim()) || 0
  }
  return 0
}

function saveOffset(offset) {
  fs.writeFileSync(OFFSET_FILE, offset.toString())
}

function getPart() {
  if (fs.existsSync(PART_FILE)) {
    return parseInt(fs.readFileSync(PART_FILE, 'utf8').trim()) || 1
  }
  return 1
}

function savePart(part) {
  fs.writeFileSync(PART_FILE, part.toString())
}

async function uploadChunk(drive, pass, part) {
  try {
    const res = await drive.files.create({
      requestBody: {
        name: `mandi_dataset_part${part}.csv`,
        parents: [FOLDER_ID],
      },
      media: {
        mimeType: 'text/csv',
        body: pass,
      },
      fields: 'id',
      supportsAllDrives: true,
    })
    console.log(`✅ Drive file created: ${res.data.id} (part ${part})`)
  } catch (err) {
    console.log('❌ Drive Upload Error:', err.message)
  }
}

async function fetchData(offset, retries = 5) {
  const url = `https://api.data.gov.in/resource/${RESOURCE_ID}?api-key=${API_KEY}&format=json&limit=${LIMIT}&offset=${offset}`
  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      const res = await axios.get(url, { timeout: 120000 })
      return res.data.records || []
    } catch (err) {
      if (err.response && err.response.status === 429) {
        console.log(`⏳ Rate limited at offset ${offset}, waiting 10s...`)
        await sleep(10000)
      } else {
        console.log(
          `🔄 Retry ${attempt + 1}/${retries} at offset ${offset}: ${err.message}`,
        )
        await sleep(3000)
      }
      if (attempt === retries - 1) return []
    }
  }
  return []
}

async function start() {
  console.log(`🚀 Railway Scraper Initializing...`)

  // Get credentials from Env Vars or local files
  let credentials, token
  try {
    credentials = process.env.GOOGLE_CREDENTIALS
      ? JSON.parse(process.env.GOOGLE_CREDENTIALS)
      : JSON.parse(fs.readFileSync('credentials.json'))
    token = process.env.GOOGLE_TOKEN
      ? JSON.parse(process.env.GOOGLE_TOKEN)
      : JSON.parse(fs.readFileSync('token.json'))
  } catch (e) {
    console.error(
      '❌ Auth Error: credentials.json or token.json missing/invalid!',
    )
    process.exit(1)
  }

  const { client_secret, client_id, redirect_uris } =
    credentials.installed || credentials.web
  const oAuth2Client = new google.auth.OAuth2(
    client_id,
    client_secret,
    redirect_uris[0],
  )
  oAuth2Client.setCredentials(token)

  const drive = google.drive({ version: 'v3', auth: oAuth2Client })

  let offset = getOffset()
  let part = getPart()
  let rowCount = 0
  let headerWritten = offset > 0
  let pass = null

  console.log(`📍 Starting from offset: ${offset} | Part: ${part}`)

  while (true) {
    const tasks = []
    for (let i = 0; i < NUM_WORKERS; i++) {
      const currentOffset = offset + i * LIMIT
      tasks.push(
        fetchData(currentOffset).then((records) => ({
          offset: currentOffset,
          records,
        })),
      )
    }

    const results = await Promise.all(tasks)
    let hasData = false
    const allRecords = []

    for (const result of results) {
      if (result.records && result.records.length > 0) {
        hasData = true
        allRecords.push(...result.records)
      }
    }

    if (!hasData) {
      console.log('\n✅ All data fetched!')
      break
    }

    if (!pass) pass = new stream.PassThrough()

    if (!headerWritten && allRecords.length > 0) {
      const header = Object.keys(allRecords[0]).join(',')
      pass.write(header + '\n')
      headerWritten = true
    }

    const rows = allRecords
      .map((r) =>
        Object.values(r)
          .map((v) => `"${v}"`)
          .join(','),
      )
      .join('\n')
    pass.write(rows + '\n')

    offset += NUM_WORKERS * LIMIT
    rowCount += allRecords.length
    saveOffset(offset)

    process.stdout.write(
      `\r📊 Rows: ${offset} | Part: ${part} | Current Chunk: ${rowCount}`,
    )

    if (rowCount >= CHUNK_SIZE) {
      pass.end()
      await uploadChunk(drive, pass, part)
      console.log(`\n🎉 Chunk ${part} completed.\n`)
      part++
      savePart(part)
      rowCount = 0
      pass = new stream.PassThrough()
    }

    await sleep(1000)
  }

  if (pass) {
    pass.end()
    await uploadChunk(drive, pass, part)
  }
  console.log('\n🎊 Scraping complete!')
}

start()
