#\!/bin/bash
echo "🧹 Emergency VM Cleanup - Freeing disk space..."
sudo rm -rf /var/log/*.log
sudo journalctl --vacuum-time=1d
sudo rm -rf /tmp/*
sudo apt clean
rm -rf ~/.cache/*
rm -rf ~/.npm
rm -rf ~/.local/share/pnpm
rm -rf ~/.local/share/heroku
echo "✅ Cleanup complete\! Disk usage:"
df -h /
