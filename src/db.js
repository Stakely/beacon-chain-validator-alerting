const path = require('path')
require('dotenv').config({ path: path.join(__dirname, '../.env') })
const mysql = require('mysql')
const util = require('util')

const config = {
  host: process.env.MYSQL_HOST,
  user: process.env.MYSQL_USER,
  port: process.env.MYSQL_PORT,
  password: process.env.MYSQL_PASSWORD,
  database: process.env.MYSQL_DATABASE,
  multipleStatements: true
}

// Create a MySQL pool
const pool = mysql.createPool(config)
pool.query = util.promisify(pool.query)

// Export the pool
module.exports = pool
