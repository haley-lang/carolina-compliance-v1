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

# UTM base URLs
UTM_BASE_WEBSITE  = "https://carolinacompliancesolutions.com/"
UTM_BASE_DEMO     = "https://carolinacompliancesolutions.com/demo"
UTM_BASE_AUDIT    = "https://carolinacompliancesolutions.com/audit-prep"
