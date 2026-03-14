const axios = require('axios')
const fs = require('fs')
const path = require('path')
const stream = require('stream')
const { google } = require('googleapis')

// --- CONFIGURATION ---
const API_KEY = process.env.DATA_GOV_API_KEY || '579b464db66ec23bdd000001e8d75b7cb11147365a5647630c56832b'
const RESOURCE_ID = '35985678-0d79-46b4-9ed6-6f13308a1d24'
const LIMIT = 5000
const CHUNK_SIZE = 500000
const FOLDER_ID = '1TNYEd-5CCzypE-mYfSsBH7yzr9iH7_Z2'
const NUM_WORKERS = 4 

const STORAGE_DIR = process.env.RAILWAY_VOLUME_MOUNT_PATH || './data'
if (!fs.existsSync(STORAGE_DIR)) fs.mkdirSync(STORAGE_DIR, { recursive: true })

const OFFSET_FILE = path.join(STORAGE_DIR, 'offset.txt')
const PART_FILE = path.join(STORAGE_DIR, 'part.txt')
const CHUNK_FILE = path.join(STORAGE_DIR, 'chunk.txt')

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)) }

function getSavedValue(file, defaultVal) {
  if (fs.existsSync(file)) return parseInt(fs.readFileSync(file, 'utf8').trim()) || defaultVal
  return defaultVal
}

// --- MODIFIED FETCH DATA (INFINITE RETRY) ---
async function fetchData(offset) {
  const url = `https://api.data.gov.in/resource/${RESOURCE_ID}?api-key=${API_KEY}&format=json&limit=${LIMIT}&offset=${offset}`
  
  while (true) { // Yeh loop tab tak chalega jab tak data na mil jaye
    try {
      const res = await axios.get(url, { timeout: 120000 })
      return res.data.records || []
    } catch (err) {
      console.log(`\n🔄 Retrying offset ${offset}: ${err.message}... (Waiting for data)`)
      await sleep(10000) // Error aane par 10 second wait karega
      // Loop continue hoga, break nahi
    }
  }
}

async function uploadChunk(drive, pass, part) {
  try {
    const res = await drive.files.create({
      requestBody: { name: `mandi_dataset_part${part}.csv`, parents: [FOLDER_ID] },
      media: { mimeType: 'text/csv', body: pass },
      fields: 'id',
      supportsAllDrives: true,
    })
    console.log(`\n✅ Drive file uploaded: (part ${part}) ID: ${res.data.id}`)
    fs.writeFileSync(CHUNK_FILE, '0')
  } catch (err) {
    console.log('\n❌ Drive Upload Error:', err.message)
  }
}

async function start() {
  console.log(`🚀 Scraper Starting (Infinite Retry Mode)...`)

  let credentials, token
  try {
    credentials = process.env.GOOGLE_CREDENTIALS ? JSON.parse(process.env.GOOGLE_CREDENTIALS) : JSON.parse(fs.readFileSync('credentials.json'))
    token = process.env.GOOGLE_TOKEN ? JSON.parse(process.env.GOOGLE_TOKEN) : JSON.parse(fs.readFileSync('token.json'))
  } catch (e) { console.error('❌ Auth Error'); process.exit(1) }

  const { client_secret, client_id, redirect_uris } = credentials.installed || credentials.web
  const oAuth2Client = new google.auth.OAuth2(client_id, client_secret, redirect_uris[0])
  oAuth2Client.setCredentials(token)
  const drive = google.drive({ version: 'v3', auth: oAuth2Client })

  let offset = getSavedValue(OFFSET_FILE, 0)
  let part = getSavedValue(PART_FILE, 1)
  let rowCount = getSavedValue(CHUNK_FILE, 0)
  let headerWritten = offset > 0
  let pass = new stream.PassThrough()

  console.log(`📍 Resume Point -> Offset: ${offset} | Part: ${part} | Chunk: ${rowCount}`)

  while (true) {
    const tasks = []
    for (let i = 0; i < NUM_WORKERS; i++) {
      const currentOffset = offset + i * LIMIT
      tasks.push(fetchData(currentOffset).then(records => ({ offset: currentOffset, records })))
    }

    const results = await Promise.all(tasks)
    const allRecords = []

    for (const result of results) {
      if (result.records.length > 0) {
        allRecords.push(...result.records)
      }
    }

    // Agar real mein data khatam ho gaya (Govt database end)
    if (allRecords.length === 0) {
      console.log('\n🏁 Reach end of Database.')
      break
    }

    if (!headerWritten && allRecords.length > 0) {
      pass.write(Object.keys(allRecords[0]).join(',') + '\n')
      headerWritten = true
    }

    const rows = allRecords.map(r => Object.values(r).map(v => `"${v}"`).join(',')).join('\n')
    pass.write(rows + '\n')

    offset += NUM_WORKERS * LIMIT
    rowCount += allRecords.length
    
    fs.writeFileSync(OFFSET_FILE, offset.toString())
    fs.writeFileSync(CHUNK_FILE, rowCount.toString()) 

    process.stdout.write(`\r📊 Total: ${offset} | Part: ${part} | Chunk: ${rowCount}`)

    if (rowCount >= CHUNK_SIZE) {
      pass.end()
      await uploadChunk(drive, pass, part)
      part++
      fs.writeFileSync(PART_FILE, part.toString())
      rowCount = 0
      fs.writeFileSync(CHUNK_FILE, '0')
      pass = new stream.PassThrough()
    }
    await sleep(2000)
  }

  if (rowCount > 0) {
    pass.end()
    await uploadChunk(drive, pass, part)
  }
}

start()