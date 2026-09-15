# Deployment Guide

This service runs on DigitalOcean App Platform, reading/writing Parquet files to DO Spaces.

## Prerequisites

- [`doctl`](https://docs.digitalocean.com/reference/doctl/how-to/install/) installed and authenticated (`doctl auth init`)
- A DO Spaces bucket and access key already created (see below)

---

## 1. Create a Spaces Bucket and Access Keys

`doctl` does not support creating Spaces buckets or access keys directly.
Do this via the control panel:

1. **Create a Space:** Dashboard → Spaces Object Storage → Create a Space
   - Choose region `nyc3`, name it (e.g. `newbuck`)
2. **Create access keys:** Dashboard → API → Spaces Keys → Generate New Key
   - Copy the **Key** and **Secret** immediately — the secret is shown only once

---

## 2. Create the App

```bash
doctl apps create --spec .do/app.yaml
```

This creates the app but the two SECRET env vars (`DO_SPACES_KEY`, `DO_SPACES_SECRET`)
will be unset. Set them in the next step.

---

## 3. Set Secret Environment Variables

Get your app ID first:

```bash
doctl apps list
```

Then update the secrets (replace `<APP_ID>`, `<KEY>`, `<SECRET>`):

```bash
doctl apps update <APP_ID> --spec .do/app.yaml
```

Or set them directly via the control panel:
App Platform → your app → Settings → Environment Variables → edit `DO_SPACES_KEY` and `DO_SPACES_SECRET`.

---

## 4. Get the Deployed App URL

```bash
doctl apps get <APP_ID> --format DefaultIngress
```

Or just:

```bash
doctl apps list
```

The URL will be in the `Live URL` column.

---

## 5. Verify the Health Check

```bash
curl https://<YOUR_APP_URL>/health
```

Expected response:

```json
{
  "status": "ok",
  "buffer_size": 0,
  "last_flush_at": null,
  "last_flush_success": null,
  "last_flush_event_count": 0,
  "consecutive_flush_failures": 0
}
```

---

## 6. Tail Logs

```bash
doctl apps logs <APP_ID> --type=RUN --follow
```

---

## 7. Redeploy After a Code Change

Push to the connected branch (App Platform auto-deploys on push), or trigger manually:

```bash
doctl apps create-deployment <APP_ID>
```

---

## Deployed URL

_To be filled in once live._
