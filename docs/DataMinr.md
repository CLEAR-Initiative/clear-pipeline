# Dataminr Documentation

## 1. Authentication API

The **Authentication API** provides the access tokens required to call other Dataminr APIs.

- **Authentication method**: Each request must include a Client ID and Client Secret issued by Dataminr. If API keys have been provisioned for your account, you can access them by navigating to API Keys. Contact your Customer Success Manager if you need keys provisioned.
- **Token lifecycle**: Access tokens expire after 4 hours. Once expired, you must request a new token before making further API calls.
- **Server-based applications**:
  - Integrators receive a Client ID and Client Secret tied to a single Dataminr Web Application user account.
  - This account must be initialized and configured in the Web Application.
  - At least one valid Alert List must be configured before API calls can be made.
- **Usage**: Include the issued token in the `Authorization` header of all subsequent API requests.

---

**Quick Start cURL**

To get started quickly, use the following cURL and replace the placeholder values.

```
curl -X 'POST' \
  'https://api.dataminr.com/auth/v1/token' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d 'grant_type=api_key&client_id=<YOUR_CLIENT_ID>&client_secret=<YOUR_CLIENT_SECRET>'
```

## **2. First Alert - API Alert Delivery Method**

# Quick Start Guide

# API Alert Delivery Method

Provides access to real-time event data from First Alert. Use it to retrieve alerts across configured alert lists.

# Get Alerts (`GET /v1/alerts`)

- Returns alerts from configured lists on the account.
- Response includes alert objects plus `nextPage` links for navigation.
- Authentication: Requires a valid bearer token in the `Authorization` header (see [Authentication API](https://developer.dataminr.com/s/communityasset/a5BPo000000Q2cTMAS/dataminrplatformauthenticationapi?groupId=4ced253c-277d-4a72-943f-b3fdbefedd89&assetId=dataminrplatformauthenticationapi&minorVersion=1.0) for details).

### Quick Start cURL

To get started quickly, use the following cURL and replace the placeholder values.

```
curl -X 'GET' \
  'https://api.dataminr.com/firstalert/v1/alerts' \
  -H 'accept: application/json' \
  -H 'Authorization: Bearer <YOUR_ACCESS_TOKEN>'
```

---

# API Alert Delivery Method Implementation Best Practices

The First API alert delivery method is a pull-based API, meaning that you have the ability to have alerts delivered at an interval of your choosing. Depending on system limitations and requirements, requests can be made as quickly as every few seconds. Generally, due to the importance of real-time breaking news alert delivery, we suggest that alerts are pulled every 15 seconds. In situations where system performance is a priority, you may pull alerts less frequently. However, make sure that alerts are delivered frequently enough relative to your alert settings.

---

# API Migration Guide

This guide explains how to migrate from the legacy `firstalert-api.dataminr.com/alerts/1/alerts` endpoint to `api.dataminr.com/firstalert/v1/alerts`. It details endpoint/param changes, pagination, field mappings, and breaking schema differences.

# 1. Endpoint & Request Parameters

| **Area**           | **Old**                                                                | **New**                                                  | **Notes**                                             |
| ------------------ | ---------------------------------------------------------------------- | -------------------------------------------------------- | ----------------------------------------------------- |
| **Base URL**       | `https://firstalert-api.dataminr.com/alerts/1/alerts`                  | `https://api.dataminr.com/firstalert/v1/alerts`          | New API host & versioning.                            |
| **`alertVersion`** | **required**                                                           | **deprecated** (not required)                            | Remove this param going forward.                      |
| **Pagination**     | not exposed via hypermedia; required manual processing of cursor value | `nextPage` link (cursor already encoded for ease of use) | New API provides ready-to-use pagination links.       |
| **`pageSize`**     | —                                                                      | optional (when enabled)                                  | Controls results per page in new API responses/links. |

# 2. Field-by-Field Mapping

The table lists changes between the two responses.

| **Old field (path)**                                                                   | **New field (path)**                                                                                   | **Change**                                 |
| -------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------ | ------------------------------------------ |
| `eventTime` (epoch ms)                                                                 | `alertTimestamp` (ISO-8601 string)                                                                     | **Renamed + format change** (epoch → ISO). |
| `firstAlertURL`                                                                        | `dataminrAlertUrl`                                                                                     | **Renamed**.                               |
| `publicPost.link`                                                                      | `publicPost.href` (+ optional `publicPost.media[]`)                                                    | **Renamed** + **media** support added.     |
| `alertLists[].name`                                                                    | `listsMatched[].name`                                                                                  | **Renamed container & field**.             |
| `linkedAlerts[].parentId`                                                              | `linkedAlerts[].parentAlertId`                                                                         | **Renamed** parent key.                    |
| `estimatedEventLocation` is an **array**: `[name, lat, long, probabilityRadius, MGRS]` | `estimatedEventLocation` is an **object**: `{ name, coordinates:[lat, lon], probabilityRadius, MGRS }` | **Structure change** (array → object).     |

**Breaking changes to highlight**

- **Timestamps** moved from `eventTime` (epoch) to `alertTimestamp`\* (ISO-8601)\*\*.
- **Estimated location** changed from **array** to **object**; **coordinates** are explicitly labeled and in `[lat, lon]` order.
- **List fields** moved from `alertLists[].name` to `listsMatched[].{id,name,topicIds[]}`.
- **Linked alert parent key** renamed (`parentId` → `parentAlertId`).
- Multiple **field renames** (`firstAlertURL` → `dataminrAlertUrl`, `publicPost.link` → `publicPost.href`).

# 3. EstimatedEventLocation Migration

```json
### Old (array)

"estimatedEventLocation": [
  "LaGuardia Airport, East Elmhurst, NY 11371, USA",
  "40.7766422",
  "-73.8742467",
  "0.9617059890164544",
  "18TWL 9500 1457"
]

### New (object)

"estimatedEventLocation": {
  "name": "Paris, Île-de-France, FRA",
  "coordinates": [48.856372814, 2.352529617],
  "probabilityRadius": 0.1609344,
  "MGRS": "31UDQ 5251 1169"
}
```

**Action**: update parsers to read object properties; `coordinates now explicitly ordered as`[lat, long]`.

# 4. Removed / Deprecated / Renamed Fields

**Deprecated parameter**

- `alertVersion` — remove from requests.

**Renamed fields**

- `firstAlertURL` → dataminrAlertUrl
- `publicPost.link` → `publicPost.href`
- `linkedAlerts[].parentId` → `linkedAlerts[].parentAlertId`

**Structural change**

- `estimatedEventLocation` array → object (see §3)

# 5. End-to-End Example Mapping

| **Old → New**     | **Example**                                                               |
| ----------------- | ------------------------------------------------------------------------- |
| **Timestamp**     | `eventTime: 1761759511019` → `alertTimestamp: "2025-10-29T17:48:45.503Z"` |
| **Alert URL**     | `firstAlertURL` → `dataminrAlertUrl`                                      |
| **Source link**   | `publicPost.link` → `publicPost.href` (+ `media[]`)                       |
| **Lists**         | `alertLists[].name` → `listsMatched[].{id,name,topicIds[]}`               |
| **Linked parent** | `linkedAlerts[].parentId` → `linkedAlerts[].parentAlertId`                |
| **Location**      | `estimatedEventLocation` (array) → object with `coordinates:[lat,lon]`    |

# Parameter Comparison

| **Parameter**  | **Old API** | **New API**      | **Notes**                 |
| -------------- | ----------- | ---------------- | ------------------------- |
| `alertVersion` | required    | deprecated       | Remove                    |
| `pageSize`     | —           | optional         | Controls results per page |
| Pagination     | Manual      | `nextPage` token | Follow returned link      |

# 6. Migration Checklist

1. Remove alertVersion from requests.
2. Convert eventTime (epoch) → alertTimestamp (ISO).
3. Rename firstAlertURL → dataminrAlertUrl.
4. Rename publicPost.link → publicPost.href; support optional media[].
5. Migrate alertLists[].name → listsMatched[].{id,name,topicIds[]}.
6. Rename linkedAlerts[].parentId → parentAlertId.
7. Update location parser (array → object with coordinates:[lat,lon]).
8. Use nextPage for pagination (no manual offsets).

# 7. Summary

The new API streamlines the payload with ISO timestamps, structured locations, and hypermedia pagination—while deprecating alertVersion. Expect simpler polling logic and cleaner data typing across clients.

# Appendix:

### Alert data structure (new API)

```JSON
{
  "alertId": "string (required)",
  "alertTimestamp": "ISO-8601 string (required)",

  "estimatedEventLocation": {
    "name": "string",
    "coordinates": ["latitude (float)", "longitude (float)"],
    "probabilityRadius": "float",
    "MGRS": "string (grid reference)"
  },

  "alertType": {
    "name": "string (criticality label)"
  },

  "headline": "string",

  "subHeadline": {
    "title": "string",
    "subHeadlines": "string"
  },

  "publicPost": {
    "href": "string (link)",
    "text": "string",
    "translatedText": "string",
    "media": ["array of media URLs"]
  },

  "dataminrAlertUrl": "string",

  "listsMatched": [
    {
      "name": "string (category)",
      "topicIds": ["string"]
    }
  ],

  "alertTopics": [
    {
      "name": "string",
      "id": "string"
    }
  ],

  "linkedAlerts": [
    {
      "parentAlertId": "string",
      "count": "integer"
    }
  ],

  "termsOfUse": "string",

  "intelAgents": [
    {
      "summary": [
        {
          "title": "string",
          "content": ["string (first element used)"]
        }
      ]
    }
  ],

  "eventCorroboration": {
    "summary": [
      {
        "content": "string"
      }
    ]
  },

  "liveBrief": [
    {
      "summary": "string"
    }
  ]
}
```

### Sample code for retrieving data [only for reference]

```python
    def __init__(self, source_model):
        """Initialize Dataminr source with metadata."""
        super().__init__(source_model)
        self.base_url = "https://api.dataminr.com"
        self.auth_url = "https://gateway.dataminr.com/auth/2/token"

    def get_required_env_vars(self) -> list[str]:
        """Dataminr requires OAuth2 client credentials."""
        return ["DATAMINR_API_CLIENT_ID", "DATAMINR_API_CLIENT_SECRET"]

    def get_test_parameters(self) -> dict:
        """Use limited pagination for stable testing.

        Note: The new API uses nextPage-based pagination.
        For production streaming, set continuous_streaming=True.
        """
        return {
            "max_requests": 1,  # Limit pagination for testing
        }

    def test_authentication(self) -> dict[str, Any]:
        """Test Dataminr authentication flow."""
        base_result = super().test_authentication()
        if base_result["status"] != "success":
            return base_result

        try:
            # Test actual authentication
            access_token = self.get_access_token()
            return {"status": "success" if access_token else "failed", "token_obtained": access_token is not None, "auth_endpoint": self.auth_url}
        except Exception as e:
            return {"status": "failed", "error": str(e)}

    def get_access_token(self) -> str | None:
        """Get access token from Dataminr OAuth2 token endpoint."""
        try:
            # Get credentials from environment
            client_id = os.getenv("DATAMINR_API_CLIENT_ID")
            client_secret = os.getenv("DATAMINR_API_CLIENT_SECRET")

            if not client_id or not client_secret:
                self.log_error("DATAMINR_API_CLIENT_ID or DATAMINR_API_CLIENT_SECRET environment variables not set")
                return None

            self.log_info(f"Authenticating with Client ID: {client_id}")

            # Make OAuth2 authentication request
            headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
            }
            data = {
                "grant_type": "api_key",
                "client_id": client_id,
                "client_secret": client_secret,
            }

            response = requests.post(self.auth_url, headers=headers, data=data, timeout=30)
            response.raise_for_status()

            auth_data = response.json()
            access_token = auth_data.get("dmaToken")

            if access_token:
                self.log_info("Successfully obtained access token")

                # expire is in milliseconds since epoch
                expiration_time = auth_data.get("expire")
                if expiration_time:
                    current_time = timezone.now().timestamp()
                    expires_in = int(expiration_time / 1000 - current_time)
                else:
                    expires_in = 3600  # Default 1 hour

                self.store_auth_token(
                    access_token=access_token,
                    token_type="Bearer",
                    expires_in=expires_in,
                )

                return access_token
            else:
                self.log_error("No access token in auth response")
                return None

        except requests.exceptions.RequestException as e:
            self.log_error("Authentication failed", error=e)
            return None
        except Exception as e:
            self.log_error("Unexpected error during authentication", error=e)
            return None

    def get(self, variable: Variable, **kwargs) -> bool:
        """Retrieve raw Dataminr alerts data for a variable using nextPage pagination."""
        try:
            self.log_info(f"Starting Dataminr data retrieval for {variable.code}")

            # Get valid access token
            access_token = self.get_valid_access_token()
            if not access_token:
                access_token = self.get_access_token()
                if not access_token:
                    self.log_error("Failed to obtain access token")
                    return False

            # Get stored token to check token type
            token_record = self.get_auth_token()
            token_type = token_record.token_type if token_record else "Bearer"

            headers = {
                "Authorization": f"{token_type} {access_token}",
                "Accept": "application/json",
            }

            # Build initial request URL and parameters
            alerts_url = f"{self.base_url}/firstalert/v1/alerts"
            params = {}

            if kwargs.get("pageSize"):
                params["pageSize"] = kwargs["pageSize"]

            self.log_info(f"API parameters: {params}")

            all_alerts = []
            total_requests = 0
            max_requests = kwargs.get("max_requests", 5)  # Limit requests to prevent infinite loops

            while total_requests < max_requests:
                total_requests += 1

                self.log_info(f"Request {total_requests}: {alerts_url}")
                response = requests.get(alerts_url, params=params, headers=headers, timeout=60)
                response.raise_for_status()

                # Parse response
                data = response.json()
                alerts = data.get("alerts", [])
                next_page = data.get("nextPage")  # Ready-to-use pagination URL

                self.log_info(f"Retrieved {len(alerts)} alerts, nextPage: {'yes' if next_page else 'None'}")

                if alerts:
                    all_alerts.extend(alerts)

                # Check if we have more data
                if not next_page or not alerts:
                    self.log_info("No more alerts to retrieve")
                    break

                # For continuous streaming, follow nextPage links
                if not kwargs.get("continuous_streaming", False):
                    break

                # Use the nextPage URL directly (already includes cursor)
                alerts_url = next_page
                params = {}  # nextPage URL already contains all parameters

            # Prepare final response data
            final_data = {
                "alerts": all_alerts,
                "total_requests": total_requests,
                "last_next_page": data.get("nextPage") if "data" in locals() else None,
                "retrieved_at": timezone.now().isoformat(),
            }

            self.log_info(f"Retrieved total of {len(all_alerts)} alerts in {total_requests} requests")

            # Store new records count for signal handling
            self.new_records_count = len(all_alerts)

            # Save raw data to file
            raw_file_path = self.get_raw_data_path(variable, ".json")
            with open(raw_file_path, "w", encoding="utf-8") as f:
                json.dump(final_data, f, indent=2, ensure_ascii=False)

            self.log_info(f"Raw data saved to: {raw_file_path}")
            return True

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                # Token expired, clear it
                self.clear_auth_token()
                self.log_error("Authentication failed (401), cleared stored tokens")
            elif e.response.status_code == 429:
                self.log_error("Rate limit exceeded (429) - API allows 180 requests per 10 minutes")
            else:
                self.log_error(f"HTTP error {e.response.status_code}", error=e)
            return False
        except requests.exceptions.RequestException as e:
            self.log_error("Request failed", error=e)
            return False
        except Exception as e:
            self.log_error("Unexpected error during data retrieval", error=e)
            return False
```
