const axios = require('axios')
const fs = require('fs')
const stream = require('stream')
const { google } = require('googleapis')

const API_KEY = '579b464db66ec23bdd000001e8d75b7cb11147365a5647630c56832b'
const RESOURCE_ID = '35985678-0d79-46b4-9ed6-6f13308a1d24'

const LIMIT = 5000
const CHUNK_SIZE = 500000
const FOLDER_ID = '1TNYEd-5CCzypE-mYfSsBH7yzr9iH7_Z2'
const NUM_WORKERS = 1 // 2 workers से smooth API calls

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms))
}

function getOffset() {
  if (fs.existsSync('offset.txt')) {
    return parseInt(fs.readFileSync('offset.txt', 'utf8').trim())
  }
  return 0
}

function saveOffset(offset) {
  fs.writeFileSync('offset.txt', offset.toString())
}

function getPart() {
  if (fs.existsSync('part.txt')) {
    return parseInt(fs.readFileSync('part.txt', 'utf8').trim())
  }
  return 1
}

function savePart(part) {
  fs.writeFileSync('part.txt', part.toString())
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

    console.log('✅ Drive file created:', res.data.id, `(part ${part})`)
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
        console.log(`⏳ Rate limited at offset ${offset}, waiting...`)
        await sleep(8000)
      } else if (err.code === 'ECONNRESET' || err.code === 'ETIMEDOUT') {
        console.log(`🔄 Retry ${attempt + 1}/${retries} at offset ${offset}`)
        await sleep(2000)
      } else {
        console.log(`❌ Error at offset ${offset}:`, err.message)
        await sleep(2000)
      }

      if (attempt === retries - 1) return []
    }
  }
  return []
}

async function start() {
  // ⭐ OAuth authentication
  if (!fs.existsSync('credentials.json')) {
    console.log('❌ credentials.json file missing')
    return
  }

  if (!fs.existsSync('token.json')) {
    console.log('❌ token.json file missing')
    return
  }

  const credentials = JSON.parse(fs.readFileSync('credentials.json'))
  const token = JSON.parse(fs.readFileSync('token.json'))

  const { client_secret, client_id, redirect_uris } =
    credentials.installed || credentials.web

  const oAuth2Client = new google.auth.OAuth2(
    client_id,
    client_secret,
    redirect_uris[0],
  )

  oAuth2Client.setCredentials(token)

  const drive = google.drive({
    version: 'v3',
    auth: oAuth2Client,
  })

  let offset = getOffset()
  let part = getPart()
  let rowCount = 0
  let headerWritten = offset > 0

  let pass = null // Don't create stream yet

  console.log(`🚀 Multi-Worker Scraper Started!`)
  console.log(`📍 Starting from offset: ${offset}`)
  console.log(`📦 Part number: ${part}`)
  console.log(`👷 Using ${NUM_WORKERS} concurrent workers\n`)

  while (true) {
    // Create concurrent fetch tasks
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

    // Wait for all workers to complete
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

    // Create new stream if not exists
    if (!pass) {
      pass = new stream.PassThrough()
    }

    // Write header only once
    if (!headerWritten && allRecords.length > 0) {
      const header = Object.keys(allRecords[0]).join(',')
      pass.write(header + '\n')
      headerWritten = true
    }

    // Write CSV rows
    const rows = allRecords.map((r) => Object.values(r).join(',')).join('\n')
    pass.write(rows + '\n')

    offset += NUM_WORKERS * LIMIT
    rowCount += allRecords.length

    saveOffset(offset)

    console.log(
      `📊 Total rows processed: ${offset} | Current part: ${part} | Rows in part: ${rowCount}`,
    )

    // Check if we need to upload and start new part
    if (rowCount >= CHUNK_SIZE) {
      if (pass) {
        pass.end()
        // Wait for upload to complete before starting new part
        await uploadChunk(drive, pass, part)
      }
      console.log(`\n🎉 Chunk ${part} completed with ${rowCount} rows\n`)

      part++
      savePart(part)
      rowCount = 0

      pass = new stream.PassThrough()
    }

    // Wait between batches to avoid API overload
    await sleep(500)
  }

  if (pass) {
    pass.end()
    await uploadChunk(drive, pass, part)
  }
  console.log('\n🎊 Scraping complete!')
}

start()
