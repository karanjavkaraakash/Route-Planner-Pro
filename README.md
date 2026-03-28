# SeaRoute — Maritime Voyage Planner

## 🚀 Deploy Online FREE (Render.com) — No credit card needed

### Step 1 — Create a GitHub account
Go to https://github.com and sign up (free). Skip if you already have one.

### Step 2 — Create a new GitHub repository
1. Click the **+** button (top right) → **New repository**
2. Name it: `searoute`
3. Set to **Public**
4. Click **Create repository**

### Step 3 — Upload your files
On the new repository page, click **uploading an existing file** (or drag and drop).
Upload ALL of these files:
- `server.py`
- `index.html`
- `requirements.txt`
- `Procfile`
- `render.yaml`

Click **Commit changes**.

### Step 4 — Deploy on Render
1. Go to https://render.com and sign up with your GitHub account (free, no card)
2. Click **New +** → **Web Service**
3. Select your `searoute` GitHub repository
4. Render auto-detects everything. Just confirm:
   - **Build Command:** `pip install -r requirements.txt`
   - **Start Command:** `gunicorn server:app`
5. Click **Create Web Service**

### Step 5 — Get your URL
Render will build and deploy in ~2 minutes.
You'll get a URL like: `https://searoute-xxxx.onrender.com`

**That's it.** Open that URL from any browser, anywhere, on any device.

---

## 💻 Run Locally (alternative)

```
pip install flask searoute gunicorn
python server.py
```
Open: http://localhost:5050

---

## Notes
- Free Render tier: service sleeps after 15 min inactivity, wakes in ~30 sec on next visit
- To keep it always-on: upgrade to Render's $7/month Starter plan
- The MARNET routing network (searoute-py) is bundled — no external API calls needed
