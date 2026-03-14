const axios = require('axios')
const fs = require('fs')
const stream = require('stream')
const { google } = require('googleapis')
const mongoose = require('mongoose')

// --- CONFIGURATION ---
const API_KEY = '579b464db66ec23bdd000001e168306278c84e916ff43065bb362867' // Nayi Key Updated
const RESOURCE_ID = '35985678-0d79-46b4-9ed6-6f13308a1d24'
const MONGO_URI = process.env.MONGO_URI || 'mongodb+srv://khushchouhan9680_db_user:9680796461@cluster0.2xwtrmi.mongodb.net/mandi_scraper?retryWrites=true&w=majority'
const FOLDER_ID = '1TNYEd-5CCzypE-mYfSsBH7yzr9iH7_Z2'
const LIMIT = 5000

const STATES = [
  "Andaman and Nicobar", "Andhra Pradesh", "Arunachal Pradesh", "Assam", "Bihar",
  "Chandigarh", "Chattisgarh", "Dadra and Nagar Haveli", "Daman and Diu", "Delhi",
  "Goa", "Gujarat", "Haryana", "Himachal Pradesh", "Jammu and Kashmir", "Jharkhand",
  "Karnataka", "Kerala", "Madhya Pradesh", "Maharashtra", "Manipur", "Meghalaya",
  "Mizoram", "Nagaland", "Odisha", "Puducherry", "Punjab", "Rajasthan", "Sikkim",
  "Tamil Nadu", "Telangana", "Tripura", "Uttar Pradesh", "Uttrakhand", "West Bengal"
]

const progressSchema = new mongoose.Schema({
  id: { type: String, default: 'scraper_progress' },
  stateIndex: { type: Number, default: 0 },
  offset: { type: Number, default: 0 }
})
const Progress = mongoose.model('Progress', progressSchema)

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)) }

// --- FETCH DATA WITH INFINITE RETRY ---
async function fetchData(offset, stateName) {
  const url = `https://api.data.gov.in/resource/${RESOURCE_ID}?api-key=${API_KEY}&format=json&limit=${LIMIT}&offset=${offset}&filters[state]=${encodeURIComponent(stateName)}`
  while (true) {
    try {
      const res = await axios.get(url, { timeout: 60000 })
      return res.data.records || []
    } catch (err) {
      process.stdout.write(`\n⏳ [${new Date().toLocaleTimeString()}] API Busy at ${stateName}. Retrying...`)
      await sleep(30000)
    }
  }
}

// --- GOOGLE DRIVE UPLOAD ---
async function uploadChunk(drive, pass, stateName) {
  try {
    const fileName = `mandi_${stateName.replace(/ /g, '_')}_FULL.csv`;
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
  console.log(`🚀 Scraper Started! Har State ki alag file Google Drive par jayegi.\n`)

  let prog = await Progress.findOne({ id: 'scraper_progress' })
  if (!prog) prog = await Progress.create({ id: 'scraper_progress' })

  // Auth setup (Local files or Env)
  let credentials, token;
  try {
    credentials = process.env.GOOGLE_CREDENTIALS ? JSON.parse(process.env.GOOGLE_CREDENTIALS) : JSON.parse(fs.readFileSync('credentials.json', 'utf8'));
    token = process.env.GOOGLE_TOKEN ? JSON.parse(process.env.GOOGLE_TOKEN) : JSON.parse(fs.readFileSync('token.json', 'utf8'));
  } catch (e) {
    console.error('❌ Error: credentials.json or token.json missing locally!');
    process.exit(1);
  }

  const { client_secret, client_id, redirect_uris } = credentials.installed || credentials.web
  const oAuth2Client = new google.auth.OAuth2(client_id, client_secret, redirect_uris[0])
  oAuth2Client.setCredentials(token)
  const drive = google.drive({ version: 'v3', auth: oAuth2Client })

  let { stateIndex, offset } = prog

  while (stateIndex < STATES.length) {
    const currentState = STATES[stateIndex]
    console.log(`\n🌍 --- Processing State: ${currentState} (${stateIndex + 1}/${STATES.length}) ---`)

    let pass = new stream.PassThrough()
    let headerWritten = false
    let stateRowCount = 0

    // Nayi file ka upload stream shuru karein
    const uploadPromise = uploadChunk(drive, pass, currentState)

    while (true) {
      const records = await fetchData(offset, currentState)

      if (records.length === 0) {
        // Double check taaki data miss na ho
        await sleep(20000)
        const check = await fetchData(offset, currentState)
        if (check.length === 0) break
      }

      // --- LIVE CONSOLE LOGGING ---
      stateRowCount += records.length;
      console.log(`[${new Date().toLocaleTimeString()}] 📥 Received: +${records.length} rows | Total for ${currentState}: ${stateRowCount} | Offset: ${offset}`);

      if (!headerWritten) {
        pass.write(Object.keys(records[0]).join(',') + '\n')
        headerWritten = true
      }

      const rows = records.map(r => Object.values(r).map(v => `"${v}"`).join(',')).join('\n')
      pass.write(rows + '\n')

      offset += LIMIT

      // Update progress in DB
      await Progress.updateOne({ id: 'scraper_progress' }, { offset, stateIndex })

      await sleep(2000) // Delay to prevent IP block
    }

    // State khatam, stream close karein
    pass.end()
    await uploadPromise

    console.log(`\n🎉 Finished ${currentState}. Moving to next state...`)

    // Agli state ke liye reset
    stateIndex++
    offset = 0
    await Progress.updateOne({ id: 'scraper_progress' }, { stateIndex, offset: 0 })
  }
  console.log("\n🏁 MISSION ACCOMPLISHED: 7.7 CR DATA SCRAPED STATE-WISE!")
}

start().catch(err => console.error(err))
