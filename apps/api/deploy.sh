#!/bin/bash
# Deploy HireTrack Sync API to Windows Server
# Run from repo root: ./apps/api/deploy.sh

SERVER="Admin@100.100.139.110"  # Tailscale IP
REMOTE_PATH="E:/hiretrack-flask-api/server"

echo "=========================================="
echo "Deploying to $SERVER"
echo "=========================================="

# Copy server files
echo "📦 Copying files..."
scp apps/api/app.py apps/api/requirements.txt $SERVER:"$REMOTE_PATH/"

# Check if running as service or manual
echo ""
echo "⚠️  NOTE: You need to restart the app manually!"
echo ""
echo "If running as a service (NSSM installed):"
echo "  ssh $SERVER \"nssm restart HireTrackFlaskApi\""
echo ""
echo "If running manually, SSH in and:"
echo "  1. Stop the current app.py (Ctrl+C or kill the process)"
echo "  2. cd E:\\hiretrack-flask-api\\server"
echo "  3. python app.py"
echo ""

# Test connection
echo "🔍 Testing API health..."
sleep 1
curl -s http://100.100.139.110:5003/health && echo "" || echo "❌ API not responding (restart needed)"

echo ""
echo "Done!"
