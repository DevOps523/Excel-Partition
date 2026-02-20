module.exports = {
  apps: [
    {
      name: "roster-generator",
      script: "server.js",
      instances: 1,
      exec_mode: "fork",
      watch: false,
      env: {
        NODE_ENV: "production",
        PORT: 3000
      }
    }
  ]
};