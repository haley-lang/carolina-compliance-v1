"""
Airtable table IDs for the Marketing feature.
All marketing state lives in Airtable (CLAUDE.md rule).
"""

MARKETING_BASE_ID = "appCGgww0Pt7KE04u"

# Table IDs
MARKETING_SIGNALS_TABLE_ID  = "tblDf7xueKpkecjm0"
MARKETING_IDEAS_TABLE_ID    = "tblEUTFIA4SU7lwcu"
CONTENT_QUEUE_TABLE_ID      = "tblSyKJ7TcmMlJK9s"
RESULTS_LOG_TABLE_ID        = "tblXS22afcwoBKWpB"
CONVERSATIONS_TABLE_ID      = "tbl0Gnwu7IKB10386"
PLAYBOOK_TABLE_ID           = "tbllUCn5EiHn2o6cy"

# Marketing Signals fields
MS_SUMMARY       = "Summary"
MS_SOURCE        = "Source"
MS_RAW_REF_ID    = "Raw Reference ID"
MS_CREATED_AT    = "Created At"

# Marketing Ideas fields
MI_IDEA          = "Idea"
MI_WHY_STORY     = "Why It's a Story"
MI_AUDIENCE      = "Audience"
MI_TOPIC         = "Topic"
MI_STATUS        = "Status"
MI_SIGNAL_LINK   = "Signal Link"

MI_STATUS_NEW      = "New"
MI_STATUS_APPROVED = "Approved"
MI_STATUS_DISMISSED = "Dismissed"

# Content Queue fields
CQ_DRAFT_TEXT    = "Draft Text"
CQ_PLATFORM      = "Platform"
CQ_HOOKS         = "Hooks"
CQ_STATUS        = "Status"
CQ_NEEDS_BILLY   = "Needs Billy's Okay"
CQ_GUARD_RESULT  = "Compliance Guard Result"
CQ_TRACKED_LINK  = "Tracked Link"
CQ_IDEA_LINK     = "Idea Link"

CQ_STATUS_DRAFT    = "Draft"
CQ_STATUS_APPROVED = "Approved"
CQ_STATUS_POSTED   = "Posted"

# Results Log fields
RL_CONTENT_NOTE  = "Content Note"
RL_VIEWS         = "Views"
RL_COMMENTS      = "Comments"
RL_MESSAGES      = "Messages"
RL_CONVERSATIONS = "Conversations"
RL_SIGN_UPS      = "Sign-ups"
RL_NOTES         = "Notes"
RL_LOGGED_AT     = "Logged At"
RL_CONTENT_LINK  = "Content Link"

# Site Knowledge table
SITE_KNOWLEDGE_TABLE_ID = "tblsuDLKUxyojWKiM"

# Site Knowledge fields
SKB_URL          = "URL"
SKB_TITLE        = "Title"
SKB_BODY_TEXT    = "Body Text"
SKB_LAST_FETCHED = "Last Fetched"
SKB_FETCH_STATUS = "Fetch Status"

SKB_STATUS_OK      = "ok"
SKB_STATUS_ERROR   = "error"
SKB_STATUS_PENDING = "pending"

# New Content Queue fields
CQ_VOICE          = "Voice"
CQ_CTA_TYPE       = "CTA Type"
CQ_CLAIM_FLAGS    = "Claim Flags"
CQ_CHANNEL        = "Channel"
CQ_CHANNEL_STATUS = "Channel Status"

# Voice values
VOICE_FRIENDLY_NEIGHBOR = "friendly_neighbor"
VOICE_CALM_EXPERT       = "calm_expert"
VOICE_STORYTELLER       = "storyteller"
VOICES = [VOICE_FRIENDLY_NEIGHBOR, VOICE_CALM_EXPERT, VOICE_STORYTELLER]

# CTA values
CTA_MESSAGE_ME = "message_me"
CTA_SIGN_UP    = "sign_up"
CTAS = [CTA_MESSAGE_ME, CTA_SIGN_UP]

# Channel values
CHANNEL_LINKEDIN_PERSONAL         = "linkedin_personal"
CHANNEL_CCS_FACEBOOK              = "ccs_facebook"
CHANNEL_CCS_TIKTOK                = "ccs_tiktok"
CHANNEL_LOCAL_FACEBOOK_GROUP      = "local_facebook_group"
CHANNEL_CONTRACTOR_FACEBOOK_GROUP = "contractor_facebook_group"

# Channels that don't have accounts yet — displayed as a warning badge
CHANNELS_NOT_SETUP = {CHANNEL_CCS_FACEBOOK, CHANNEL_CCS_TIKTOK}
CHANNEL_STATUS_NOT_SETUP = "account not set up"

# UTM base URLs
UTM_BASE_WEBSITE  = "https://carolinacompliancesolutions.com/"
UTM_BASE_DEMO     = "https://carolinacompliancesolutions.com/demo"
UTM_BASE_AUDIT    = "https://carolinacompliancesolutions.com/audit-prep"
