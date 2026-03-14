const axios = require('axios')
const fs = require('fs')
const stream = require('stream')
const { google } = require('googleapis')
const mongoose = require('mongoose')

// --- CONFIGURATION ---
const API_KEY = '579b464db66ec23bdd000001e168306278c84e916ff43065bb362867'
const RESOURCE_ID = '35985678-0d79-46b4-9ed6-6f13308a1d24'
const MONGO_URI =
  process.env.MONGO_URI ||
  'mongodb+srv://khushchouhan9680_db_user:9680796461@cluster0.2xwtrmi.mongodb.net/mandi_scraper?retryWrites=true&w=majority'
const FOLDER_ID = '1TNYEd-5CCzypE-mYfSsBH7yzr9iH7_Z2'
const LIMIT = 2000 // Stable performance ke liye 2000 rakha hai

const YEARS = Array.from({ length: 25 }, (_, i) => (2002 + i).toString())

const progressSchema = new mongoose.Schema({
  id: { type: String, default: 'scraper_progress_v3' }, // V3 for clean start
  yearIndex: { type: Number, default: 0 },
  offset: { type: Number, default: 0 },
})
const Progress = mongoose.model('Progress', progressSchema)

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms))
}

async function fetchData(offset, year) {
  // sort add karne se API query stable ho jati hai
  const url = `https://api.data.gov.in/resource/${RESOURCE_ID}?api-key=${API_KEY}&format=json&limit=${LIMIT}&offset=${offset}&filters[arrival_date]=*${year}&sort[arrival_date]=asc`

  while (true) {
    try {
      const res = await axios.get(url, { timeout: 90000 }) // 90 seconds timeout
      return res.data.records || []
    } catch (err) {
      const time = new Date().toLocaleTimeString()
      if (err.response && err.response.status === 429) {
        process.stdout.write(
          `\n🚫 [${time}] Rate Limit Hit! Sleeping for 60s...`,
        )
        await sleep(60000)
      } else {
        process.stdout.write(
          `\n⏳ [${time}] API Busy or Timeout for Year ${year}. Retrying...`,
        )
        await sleep(20000)
      }
    }
  }
}

async function uploadYearlyFile(drive, pass, year) {
  try {
    const fileName = `mandi_all_states_${year}.csv`
    const res = await drive.files.create({
      requestBody: { name: fileName, parents: [FOLDER_ID] },
      media: { mimeType: 'text/csv', body: pass },
      fields: 'id',
      supportsAllDrives: true,
    })
    console.log(`\n✅ [FILE SAVED] ${fileName} uploaded! ID: ${res.data.id}`)
  } catch (err) {
    console.log('\n❌ Drive Error:', err.message)
  }
}

async function start() {
  await mongoose.connect(MONGO_URI)
  console.log(`🚀 STABLE YEAR-WISE SCRAPER STARTED! (2002lkdflksdlf - 2026)`)

  let credentials, token
  try {
    if (process.env.GOOGLE_CREDENTIALS) {
      credentials = JSON.parse(process.env.GOOGLE_CREDENTIALS)
      token = JSON.parse(process.env.GOOGLE_TOKEN)
      console.log('🔑 Using Auth from Environment Variables.')
    } else {
      credentials = JSON.parse(fs.readFileSync('credentials.json', 'utf8'))
      token = JSON.parse(fs.readFileSync('token.json', 'utf8'))
      console.log('📁 Using Auth from Local Files.')
    }
  } catch (e) {
    console.error('❌ FATAL ERROR: Auth data missing!')
    process.exit(1)
  }

  const client_data = credentials.installed || credentials.web
  const oAuth2Client = new google.auth.OAuth2(
    client_data.client_id,
    client_data.client_secret,
    client_data.redirect_uris[0],
  )
  oAuth2Client.setCredentials(token)
  const drive = google.drive({ version: 'v3', auth: oAuth2Client })

  let prog = await Progress.findOne({ id: 'scraper_progress_v3' })
  if (!prog) prog = await Progress.create({ id: 'scraper_progress_v3' })

  let { yearIndex, offset } = prog

  while (yearIndex < YEARS.length) {
    const currentYear = YEARS[yearIndex]
    console.log(
      `\n📅 --- Processing Year: ${currentYear} (${yearIndex + 1}/${YEARS.length}) ---`,
    )

    let pass = new stream.PassThrough(),
      headerWritten = false,
      yearRowCount = 0
    const uploadPromise = uploadYearlyFile(drive, pass, currentYear)

    while (true) {
      const records = await fetchData(offset, currentYear)

      if (records.length === 0) {
        await sleep(10000) // Verification delay
        const check = await fetchData(offset, currentYear)
        if (check.length === 0) break
      }

      yearRowCount += records.length
      process.stdout.write(
        `\r[${new Date().toLocaleTimeString()}] 📥 Year ${currentYear}: Total ${yearRowCount.toLocaleString()} | Offset: ${offset}`,
      )

      if (!headerWritten) {
        pass.write(Object.keys(records[0]).join(',') + '\n')
        headerWritten = true
      }

      const rows = records
        .map((r) =>
          Object.values(r)
            .map((v) => `"${v}"`)
            .join(','),
        )
        .join('\n')
      pass.write(rows + '\n')

      offset += LIMIT
      await Progress.updateOne(
        { id: 'scraper_progress_v3' },
        { offset, yearIndex },
      )

      await sleep(2000) // API ko saans lene ke liye 2s delay
    }

    pass.end()
    await uploadPromise

    yearIndex++
    offset = 0
    await Progress.updateOne(
      { id: 'scraper_progress_v3' },
      { yearIndex, offset: 0 },
    )
    console.log(`\n🎉 Year ${currentYear} Finished!`)
  }
  console.log('\n🏁 MISSION COMPLETE!')
}

start().catch((err) => console.error(err))
