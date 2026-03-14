const axios = require('axios')
const fs = require('fs')
const stream = require('stream')
const { google } = require('googleapis')
const mongoose = require('mongoose')

// --- CONFIGURATION ---
const API_KEY =
  process.env.DATA_GOV_API_KEY ||
  '579b464db66ec23bdd000001e8d75b7cb11147365a5647630c56832b'
const RESOURCE_ID = '35985678-0d79-46b4-9ed6-6f13308a1d24'
// Aapka MongoDB Connection String
const MONGO_URI =
  process.env.MONGO_URI ||
  'mongodb+srv://khushchouhan9680_db_user:9680796461@cluster0.2xwtrmi.mongodb.net/mandi_scraper?retryWrites=true&w=majority'

const LIMIT = 5000
const CHUNK_SIZE = 500000
const FOLDER_ID = '1TNYEd-5CCzypE-mYfSsBH7yzr9iH7_Z2'
const NUM_WORKERS = 4

// --- MONGODB SCHEMA ---
const progressSchema = new mongoose.Schema({
  id: { type: String, default: 'scraper_progress' },
  offset: { type: Number, default: 0 },
  part: { type: Number, default: 1 },
  rowCount: { type: Number, default: 0 },
})
const Progress = mongoose.model('Progress', progressSchema)

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms))
}

// --- INFINITE RETRY FETCH ---
async function fetchData(offset) {
  const url = `https://api.data.gov.in/resource/${RESOURCE_ID}?api-key=${API_KEY}&format=json&limit=${LIMIT}&offset=${offset}`
  while (true) {
    try {
      const res = await axios.get(url, { timeout: 120000 })
      return res.data.records || []
    } catch (err) {
      process.stdout.write(
        `\n🔄 Offset ${offset} is stuck (Error: ${err.message}). Retrying in 10s...`,
      )
      await sleep(10000)
    }
  }
}

async function uploadChunk(drive, pass, part) {
  try {
    const res = await drive.files.create({
      requestBody: {
        name: `mandi_dataset_part${part}.csv`,
        parents: [FOLDER_ID],
      },
      media: { mimeType: 'text/csv', body: pass },
      fields: 'id',
      supportsAllDrives: true,
    })
    console.log(`\n✅ Drive file uploaded! Part: ${part} | ID: ${res.data.id}`)
  } catch (err) {
    console.log('\n❌ Drive Upload Error:', err.message)
  }
}

async function start() {
  console.log(`🚀 Connecting to MongoDB...`)
  await mongoose.connect(MONGO_URI)
  console.log(`✅ MongoDB Connected!`)

  // Progress Loading Logic
  let state = await Progress.findOne({ id: 'scraper_progress' })

  if (!state) {
    console.log(
      '📍 No previous progress found. Starting from Part 4 / Offset 15,00,000 as requested.',
    )
    state = await Progress.create({
      id: 'scraper_progress',
      offset: 1500000,
      part: 4,
      rowCount: 0,
    })
  }

  // Auth setup (Drive)
  let credentials = JSON.parse(fs.readFileSync('credentials.json'))
  let token = JSON.parse(fs.readFileSync('token.json'))
  const { client_secret, client_id, redirect_uris } =
    credentials.installed || credentials.web
  const oAuth2Client = new google.auth.OAuth2(
    client_id,
    client_secret,
    redirect_uris[0],
  )
  oAuth2Client.setCredentials(token)
  const drive = google.drive({ version: 'v3', auth: oAuth2Client })

  let { offset, part, rowCount } = state
  let headerWritten = false
  let pass = new stream.PassThrough()

  console.log(
    `📍 Resuming from DB -> Offset: ${offset} | Part: ${part} | Rows in Chunk: ${rowCount}`,
  )

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
    const allRecords = []
    for (const result of results) {
      if (result.records.length > 0) allRecords.push(...result.records)
    }

    if (allRecords.length === 0) {
      console.log('\n🏁 Database End Reached.')
      break
    }

    if (!headerWritten && allRecords.length > 0) {
      pass.write(Object.keys(allRecords[0]).join(',') + '\n')
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

    // Save to MongoDB every batch
    await Progress.updateOne(
      { id: 'scraper_progress' },
      { offset, rowCount, part },
    )

    process.stdout.write(
      `\r📊 Rows Processed: ${offset} | Part: ${part} | Current Chunk: ${rowCount}`,
    )

    if (rowCount >= CHUNK_SIZE) {
      pass.end()
      await uploadChunk(drive, pass, part)

      part++
      rowCount = 0
      // Update DB for new part
      await Progress.updateOne(
        { id: 'scraper_progress' },
        { part, rowCount: 0 },
      )

      pass = new stream.PassThrough()
      headerWritten = false
    }
    await sleep(2000)
  }

  if (rowCount > 0) {
    pass.end()
    await uploadChunk(drive, pass, part)
  }
}

start().catch((err) => console.error('FATAL ERROR:', err))
