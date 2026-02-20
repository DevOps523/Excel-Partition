# Hostinger VPS Deployment

## 1. Server setup
```bash
sudo apt update
sudo apt install -y nginx
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt install -y nodejs
sudo npm install -g pm2
```

## 2. Upload project
```bash
cd /var/www
git clone <your-repo-url> roster-generator
cd roster-generator
npm ci
```

## 3. Configure environment
```bash
cp .env.example .env
nano .env
```
If you do not have `.env.example`, create `.env` directly and set required vars.

## 4. Start with PM2
```bash
cd /var/www/roster-generator
pm2 start ecosystem.config.js
pm2 save
pm2 startup
```

## 5. Nginx reverse proxy
Create `/etc/nginx/sites-available/roster-generator`:

```nginx
server {
    listen 80;
    server_name your-domain.com www.your-domain.com;

    client_max_body_size 1024M;

    location / {
        proxy_pass http://127.0.0.1:3000;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_cache_bypass $http_upgrade;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

Enable and reload:
```bash
sudo ln -s /etc/nginx/sites-available/roster-generator /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl reload nginx
```

## 6. HTTPS
```bash
sudo apt install -y certbot python3-certbot-nginx
sudo certbot --nginx -d your-domain.com -d www.your-domain.com
```

## 7. Updates
```bash
cd /var/www/roster-generator
git pull
npm ci
pm2 restart roster-generator
```