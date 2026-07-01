# WHOOP Circle

A private, multi-user WHOOP tracker. Each person signs in with their **own** WHOOP
account, gets a private dashboard (lifetime archive, sleep scoring, recovery/strain
patterns, decision-rating journal, and a peptide tracker), and can compare stats with
friends inside **invite-only circles**.

Circles are fully isolated: you only ever see people inside a circle you share, the
owner approves every member, and different circles never see each other. Sharing is
per-circle and opt-in.

---

## What each person gets

- **Personal dashboard** — their entire WHOOP history, synced and analyzed. Fully private.
- **Peptide tracker** — plan the week, tick each dose off (e.g. GHK-Cu 7×/week, RETA on chosen days), notes per peptide to track if it's working, weekly adherence.
- **Circles** — create or join circles by invite code; per-circle leaderboards, streaks/challenges, and achievement badges. You choose per circle what you share (health stats and peptides, separately).

## Privacy model

- No global directory. You only see co-members of circles you're an active member of.
- The circle **owner approves every join request** — nobody sees anything until approved.
- Two people only see each other if they're in the **same** circle. Put friends in separate circles to keep them fully walled off.
- Peptide sharing is **off by default** per circle.

---

## Deploy (Render + Postgres, free)

### 1. Create your WHOOP developer app
1. Go to https://developer-dashboard.whoop.com and sign in.
2. Create an app. Set **Redirect URL** to `https://YOUR-APP.onrender.com/callback`
   (you'll know the exact URL after step 2 — you can come back and fix it).
3. Enable scopes: `read:profile`, `read:recovery`, `read:cycles`, `read:sleep`, `read:workout`, `read:body_measurement`, and **offline**.
4. Copy the **Client ID** and **Client Secret**.

> Note: a WHOOP app supports **10 users** without approval. For more, submit the app for approval in the dashboard.

### 2. Deploy to Render
1. Push this folder to a GitHub repo.
2. In Render: **New → Blueprint**, pick the repo. `render.yaml` provisions the web service **and a free Postgres database** automatically.
3. When prompted, set the environment variables:
   - `WHOOP_CLIENT_ID` and `WHOOP_CLIENT_SECRET` — from step 1 (kept private on the server).
   - `APP_BASE_URL` — your Render URL, e.g. `https://whoop-circle.onrender.com`.
   - `APP_SECRET` and `DATABASE_URL` are set automatically.
4. Deploy. Once live, make sure your WHOOP app's Redirect URL matches `APP_BASE_URL` + `/callback` exactly.

### 3. Use it
- Open your Render URL → **Sign in with WHOOP** → your lifetime backfill starts automatically.
- Share the URL with friends; each signs in with their own WHOOP.
- Create a circle, share its invite code, approve friends, and compare.

---

## Run locally (optional)

```bash
pip install -r requirements.txt
export WHOOP_CLIENT_ID=... WHOOP_CLIENT_SECRET=...
# register http://localhost:8000/callback as a redirect URL in the WHOOP dashboard
uvicorn app:app --port 8000
```

Without `DATABASE_URL` it uses a local SQLite file (`whoop_circle.db`). On Render,
`DATABASE_URL` makes it use Postgres so everyone's data persists across restarts.

## Files

- `app.py` — the entire app (backend + dashboard in one file).
- `requirements.txt`, `render.yaml` — deploy config.
- `preview.html` — offline demo with sample data (not part of the deploy).

## Notes

- WHOOP's Stress Monitor metric isn't exposed in the developer API, so "stress" is reflected indirectly through HRV/recovery.
- This is a personal project, not medical advice. Peptide tracking is a logging tool only.
