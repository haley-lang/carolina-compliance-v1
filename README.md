# Carolina Compliance Solutions — COI Email Monitor

Monitors an email inbox for incoming Certificates of Insurance (COI).
When a qualifying attachment (PDF or image) is found, the file is saved
locally and a record is created in Airtable.

---

## Project Structure

```
carolina-compliance-v1/
├── main.py               # Module 1 entry point — email monitor
├── config.py             # Loads and validates environment variables
├── email_monitor.py      # IMAP connection and attachment download
├── airtable_client.py    # Airtable client for Incoming Documents table
├── extractor.py          # Module 2 — sends uploads to OpenAI, saves JSON
├── airtable_importer.py  # Module 3 — imports extracted JSON into Airtable
├── processor.py          # Module 4 — creates Policy/Certificate records from extractions
├── compliance_checker.py # Module 5 — evaluates vendor compliance status
├── task_generator.py     # Module 6 — creates follow-up tasks for expiring/expired policies
├── utils.py              # Shared helpers (logging, filename sanitization)
├── requirements.txt      # Python dependencies
├── .env.example          # Environment variable template
├── .gitignore
├── uploads/              # Downloaded COI files are saved here
└── extracted/            # Structured JSON output from Module 2
```

---

## Setup

### 1. Clone or download the project

```bash
cd carolina-compliance-v1
```

### 2. Create a virtual environment and install dependencies

```bash
python3 -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Configure environment variables

```bash
cp .env.example .env
```

Open `.env` and fill in your values:

| Variable              | Description                                              |
|-----------------------|----------------------------------------------------------|
| `IMAP_HOST`           | IMAP server hostname (e.g. `imap.gmail.com`)             |
| `IMAP_PORT`           | IMAP SSL port — usually `993`                            |
| `EMAIL_ADDRESS`       | The inbox email address to monitor                       |
| `EMAIL_PASSWORD`      | App password (see note below for Gmail)                  |
| `AIRTABLE_API_KEY`    | Your Airtable personal access token                      |
| `AIRTABLE_BASE_ID`    | Base ID — found in the Airtable URL (`appXXXXXXXX`)      |
| `AIRTABLE_TABLE_NAME` | Table name, default: `Incoming Documents`                |
| `UPLOAD_DIR`          | Local folder for saved files, default: `uploads`         |

**Gmail users:** You must use an [App Password](https://myaccount.google.com/apppasswords),
not your regular password. Enable 2-Step Verification first, then generate an app password
under Security > 2-Step Verification > App passwords.

### 4. Set up the Airtable table

In your Airtable base, create a table named **Incoming Documents** with these fields:

| Field name      | Field type         |
|-----------------|--------------------|
| Sender Email    | Single line text   |
| Subject         | Single line text   |
| Date Received   | Single line text   |
| File Names      | Long text          |
| Status          | Single select      |

Add `Pending Review` as a single-select option for the **Status** field.

---

## Running the monitor

```bash
python main.py
```

The script will:
1. Connect to your inbox via IMAP
2. Scan all **unread** emails for PDF or image attachments
3. Save qualifying files to the `uploads/` folder
4. Create one Airtable record per email containing attachments
5. Log all activity to the console

Run it on a schedule (e.g. cron, Task Scheduler, or a cloud function) to
continuously monitor the inbox.

### Example cron job (every 15 minutes)

```
*/15 * * * * /path/to/venv/bin/python /path/to/carolina-compliance-v1/main.py >> /var/log/coi_monitor.log 2>&1
```

---

## Accepted file types

`.pdf`, `.png`, `.jpg`, `.jpeg`, `.tiff`, `.tif`

To add or remove types, update `ALLOWED_EXTENSIONS` in [config.py](config.py).

---

## Module 2 — Document Extractor

Reads the newest file from `uploads/`, sends it to OpenAI GPT-4o for structured
extraction, and saves the result as JSON in `extracted/`.

### Additional setup (one-time)

**Install Poppler** (required for PDF-to-image conversion):

```bash
# macOS
brew install poppler

# Ubuntu / Debian
sudo apt-get install -y poppler-utils
```

**Install updated Python dependencies:**

```bash
source venv/bin/activate
pip install -r requirements.txt
```

Make sure your `.env` file contains:

```
OPENAI_API_KEY=sk-...
```

### Running the extractor

```bash
source venv/bin/activate
python extractor.py
```

The script will:
1. Find the most recently modified PDF, JPG, JPEG, or PNG in `uploads/`
2. Convert PDFs to images (one per page) and encode them as base64
3. Send the image(s) to GPT-4o with a structured extraction prompt
4. Parse the JSON response
5. Save the result to `extracted/<filename>.json`
6. Log a summary (document type, named insured, policy count)

### Output format

`extracted/<filename>.json`:

```json
{
  "document_type": "COI",
  "named_insured": "Acme Corp",
  "certificate_date": "01/15/2025",
  "contact_emails": ["agent@insurer.com"],
  "policies": [
    {
      "policy_type": "General Liability",
      "policy_number": "GL-123456",
      "carrier": "Hartford",
      "effective_date": "01/01/2025",
      "expiration_date": "01/01/2026",
      "coverage_limits": "$1,000,000 / $2,000,000"
    }
  ]
}
```

Fields that cannot be found are returned as `null` or `[]`. The extractor
never guesses — if a value is not clearly visible, it is omitted.

---

## Module 3 — Airtable Importer

Reads the newest JSON file from `extracted/`, maps the fields, and creates
a record in the Airtable table **Incoming Extractions**.

### Prerequisites

- Module 2 must have already run and saved at least one file to `extracted/`.
- Your `.env` must contain valid `AIRTABLE_API_KEY` and `AIRTABLE_BASE_ID`.

### Airtable table setup

In your Airtable base, create a table named **Incoming Extractions** with
these fields:

| Field name               | Field type         |
|--------------------------|--------------------|
| Source Filename          | Single line text   |
| Document Type            | Single line text   |
| Named Insured            | Single line text   |
| Contact Emails           | Long text          |
| Policies Count           | Number (integer)   |
| Raw JSON                 | Long text          |
| Extraction Processed At  | Single line text   |
| Processing Status        | Single select      |

Add `Imported` as a single-select option for the **Processing Status** field.

### Running the importer

```bash
source venv/bin/activate
python airtable_importer.py
```

The script will:
1. Find the most recently modified `.json` file in `extracted/`
2. Parse and validate the JSON
3. Map fields (document type, named insured, contact emails, policy count, raw JSON)
4. Create one record in the **Incoming Extractions** Airtable table
5. Log the new Airtable record ID to the console

---

## Module 4 — COI Processor

Reads the newest `Imported` record from **Incoming Extractions**, matches the
vendor, creates **Insurance Policy** and **Insurance Certificate** records,
and marks the extraction as `Processed` (or `Needs Review` if no vendor match).

### Prerequisites

- Module 3 must have already created at least one record in **Incoming Extractions**
  with `Processing Status = Imported`.
- A matching vendor must exist in the **Vendors** table (case-insensitive name match).

### Airtable table setup

**Vendors** table (must already exist):

| Field name  | Field type       |
|-------------|------------------|
| Vendor Name | Single line text |

**Insurance Policies** table:

| Field name                    | Field type            |
|-------------------------------|-----------------------|
| Policy Record                 | Single line text      |
| Vendor                        | Link to Vendors       |
| Policy Type                   | Single line text      |
| Policy Number                 | Single line text      |
| Carrier                       | Single line text      |
| Effective Date                | Single line text      |
| Expiration Date               | Single line text      |
| Coverage Limits               | Single line text      |
| Policy Status                 | Single select         |
| Certificate Source Filename   | Single line text      |

Add `Active` as a single-select option for **Policy Status**.

**Insurance Certificates** table:

| Field name          | Field type                      |
|---------------------|---------------------------------|
| Vendor              | Link to Vendors                 |
| Named Insured       | Single line text                |
| Source Filename     | Single line text                |
| Certificate Date    | Single line text                |
| Insurance Policies  | Link to Insurance Policies      |

### Running the processor

```bash
source venv/bin/activate
python processor.py
```

The script will:
1. Fetch the newest `Imported` record from **Incoming Extractions**
2. Parse the stored Raw JSON to extract the policies array
3. Match the Named Insured to a Vendor (case-insensitive)
4. If no vendor match → set status to `Needs Review` and stop
5. For each policy: create a record in **Insurance Policies** (skips duplicates by Policy Number)
6. Create one record in **Insurance Certificates** linked to the Vendor
7. Update the extraction record to `Processing Status = Processed`

---

## Module 5 — Compliance Checker

Evaluates every Vendor's linked Insurance Policies and sets the
`Compliance Status` field on the Vendors table.

### Status rules (evaluated in priority order)

| Condition | Status set |
|---|---|
| No policies linked to vendor | `Needs Review` |
| Any policy is missing Expiration Date | `Needs Review` |
| Any policy's Expiration Date is before today | `Expired` |
| Any policy expires within the next 30 days | `Expiring Soon` |
| All policies are current | `Compliant` |

### Prerequisites

- The **Vendors** table must have a `Compliance Status` single-select field.
- Add these options to that field: `Compliant`, `Expiring Soon`, `Expired`, `Needs Review`.
- The **Vendors** table should have a linked-record field called `Insurance Policies`
  pointing back to the Insurance Policies table (so the module can find each vendor's policies).

### Running the compliance checker

```bash
source venv/bin/activate
python compliance_checker.py
```

The script will:
1. Load all Insurance Policy records from Airtable (single batch call)
2. Load all Vendor records
3. For each vendor, find linked policies and evaluate their expiration dates
4. Update `Compliance Status` only if it has changed
5. Log a summary count of each status at the end

---

## Module 6 — Task Generator

Scans all Insurance Policies and creates follow-up tasks in the **Tasks** table
for policies that are expired or expiring within 30 days.

### Task rules

| Policy condition | Task Name | Priority |
|---|---|---|
| Expiration Date before today | `Expired policy follow-up` | `Urgent` |
| Expiration Date within 30 days | `Expiring policy renewal reminder` | `High` |
| Expiration Date missing | skipped | — |
| Already has an open task with same name | skipped (no duplicate) | — |

### Prerequisites

The **Tasks** table must exist in Airtable with these fields:

| Field name         | Field type                       |
|--------------------|----------------------------------|
| Task Name          | Single line text                 |
| Status             | Single select                    |
| Priority           | Single select                    |
| Insurance Policy   | Link to Insurance Policies       |
| Vendor             | Link to Vendors                  |
| Notes              | Long text                        |

Add `Open` as a single-select option for **Status**.
Add `Urgent` and `High` as single-select options for **Priority**.

### Running the task generator

```bash
source venv/bin/activate
python task_generator.py
```

The script will:
1. Load all Insurance Policy records (single batch call)
2. Load all existing Tasks records (single batch call, used for duplicate detection)
3. For each policy with a parseable expiration date, classify it as expired or expiring soon
4. Skip policies with no expiration date
5. Skip policies that already have an open task with the same name
6. Create a task linked to the policy and vendor for each actionable policy
7. Log a summary of tasks created, duplicates skipped, and policies with no date
