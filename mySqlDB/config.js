module.exports = {
  mysql: {
    Localhost: {
      host: process.env.LOCALHOST_MYSQL_HOST || 'localhost',
      port: process.env.LOCALHOST_MYSQL_PORT || '3306',
      user: process.env.LOCALHOST_MYSQL_USER || 'test',
      password: process.env.LOCALHOST_MYSQL_PASSWORD || 'test123',
      database: process.env.LOCALHOST_MYSQL_AUTH_DB || 'auth'
    },
    LocalhostRead: {
      host: process.env.LOCALHOST_MYSQL_HOST || '',
      port: process.env.LOCALHOST_MYSQL_PORT || '',
      user: process.env.LOCALHOST_MYSQL_USER || '',
      password: process.env.LOCALHOST_MYSQL_PASSWORD || '',
      database: process.env.LOCALHOST_MYSQL_AUTH_DB || ''
    }
  }
}