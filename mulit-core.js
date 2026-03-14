const axios = require('axios')
const fs = require('fs')
const stream = require('stream')
const { google } = require('googleapis')
const mongoose = require('mongoose')

// --- CONFIGURATION ---
const API_KEY = '579b464db66ec23bdd000001e168306278c84e916ff43065bb362867'
const RESOURCE_ID = '35985678-0d79-46b4-9ed6-6f13308a1d24'
const MONGO_URI = process.env.MONGO_URI || 'mongodb+srv://khushchouhan9680_db_user:9680796461@cluster0.2xwtrmi.mongodb.net/mandi_scraper?retryWrites=true&w=majority'
const FOLDER_ID = '1TNYEd-5CCzypE-mYfSsBH7yzr9iH7_Z2'
const LIMIT = 5000

// 2002 se 2026 tak ke saal ki list
const YEARS = Array.from({length: 25}, (_, i) => (2002 + i).toString());

const progressSchema = new mongoose.Schema({
  id: { type: String, default: 'scraper_progress_v2' }, // V2 for Year-wise
  yearIndex: { type: Number, default: 0 },
  offset: { type: Number, default: 0 }
})
const Progress = mongoose.model('Progress', progressSchema)

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)) }

// --- FETCH DATA BY YEAR ---
async function fetchData(offset, year) {
  // Wildcard '*' use kiya hai taaki date mein kahin bhi saal match ho jaye
  const url = `https://api.data.gov.in/resource/${RESOURCE_ID}?api-key=${API_KEY}&format=json&limit=${LIMIT}&offset=${offset}&filters[arrival_date]=*${year}`

  while (true) {
    try {
      const res = await axios.get(url, { timeout: 60000 })
      return res.data.records || []
    } catch (err) {
      process.stdout.write(`\n⏳ [${new Date().toLocaleTimeString()}] API Busy for Year ${year}. Retrying in 30s...`)
      await sleep(30000)
    }
  }
}

// --- GOOGLE DRIVE UPLOAD ---
async function uploadYearlyFile(drive, pass, year) {
  try {
    const fileName = `mandi_all_states_${year}.csv`;
    const res = await drive.files.create({
      requestBody: { name: fileName, parents: [FOLDER_ID] },
      media: { mimeType: 'text/csv', body: pass },
      fields: 'id',
      supportsAllDrives: true,
    })
    console.log(`\n✅ [FILE SAVED] ${fileName} is now on Google Drive! ID: ${res.data.id}`)
  } catch (err) {
    console.log('\n❌ Drive Error:', err.message)
  }
}

async function start() {
  await mongoose.connect(MONGO_URI)
  console.log(`🚀 GLOBAL YEAR-WISE SCRAPER STARTED! (2002 - 2026)`)
  console.log(`📊 Target: 7.7 Crore Historical Records\n`)

  let prog = await Progress.findOne({ id: 'scraper_progress_v2' })
  if (!prog) {
    prog = await Progress.create({ id: 'scraper_progress_v2', yearIndex: 0, offset: 0 })
    console.log("🆕 New Progress initialized at Year 2002.")
  }

  // Auth setup
  let credentials, token;
  try {
    credentials = process.env.GOOGLE_CREDENTIALS ? JSON.parse(process.env.GOOGLE_CREDENTIALS) : JSON.parse(fs.readFileSync('credentials.json', 'utf8'));
    token = process.env.GOOGLE_TOKEN ? JSON.parse(process.env.GOOGLE_TOKEN) : JSON.parse(fs.readFileSync('token.json', 'utf8'));
  } catch (e) {
    console.error('❌ Error: credentials.json or token.json missing!');
    process.exit(1);
  }

  const { client_secret, client_id, redirect_uris } = credentials.installed || credentials.web
  const oAuth2Client = new google.auth.OAuth2(client_id, client_secret, redirect_uris[0])
  oAuth2Client.setCredentials(token)
  const drive = google.drive({ version: 'v3', auth: oAuth2Client })

  let { yearIndex, offset } = prog

  while (yearIndex < YEARS.length) {
    const currentYear = YEARS[yearIndex]
    console.log(`\n📅 --- Processing Year: ${currentYear} (${yearIndex + 1}/${YEARS.length}) ---`)

    let pass = new stream.PassThrough()
    let headerWritten = false
    let yearRowCount = 0

    // Start upload stream for the year
    const uploadPromise = uploadYearlyFile(drive, pass, currentYear)

    while (true) {
      const records = await fetchData(offset, currentYear)

      if (records.length === 0) {
        await sleep(15000) // Double check delay
        const check = await fetchData(offset, currentYear)
        if (check.length === 0) break
      }

      yearRowCount += records.length;
      process.stdout.write(`\r[${new Date().toLocaleTimeString()}] 📥 Year ${currentYear}: +${records.length} | Total: ${yearRowCount.toLocaleString()} | Offset: ${offset}`);

      if (!headerWritten) {
        pass.write(Object.keys(records[0]).join(',') + '\n')
        headerWritten = true
      }

      const rows = records.map(r => Object.values(r).map(v => `"${v}"`).join(',')).join('\n')
      pass.write(rows + '\n')

      offset += LIMIT
      // Update progress
      await Progress.updateOne({ id: 'scraper_progress_v2' }, { offset, yearIndex })

      await sleep(1500) // Safety delay
    }

    // Finish year file
    pass.end()
    await uploadPromise

    console.log(`\n🎉 Finished Year ${currentYear}. Moving to next...`)

    // Move to next year
    yearIndex++
    offset = 0
    await Progress.updateOne({ id: 'scraper_progress_v2' }, { yearIndex, offset: 0 })
  }

  console.log("\n🏁 MISSION ACCOMPLISHED: ALL DATA FROM 2002-2026 SCRAPED!")
}

start().catch(err => console.error(err))
