# ClickHouse SET ROLE Test

This is a standalone test program to verify the SET ROLE functionality with your local ClickHouse server.

## Prerequisites

1. **ClickHouse Server**: Make sure you have ClickHouse running locally
2. **Go**: Version 1.21 or later
3. **Network Access**: ClickHouse should be accessible on localhost:9000

## Setup

1. **Start ClickHouse** (if not already running):
   ```bash
   # Using Docker
   docker run -d --name clickhouse-server -p 9000:9000 -p 8123:8123 clickhouse/clickhouse-server:latest
   
   # Or using your local installation
   clickhouse-server
   ```

2. **Update connection settings** in `test_set_role.go`:
   - Change `localhost:9000` if your ClickHouse runs on a different host/port
   - Update username/password if needed
   - Modify database name if different from "default"

## Run the Test

```bash
# Install dependencies
go mod tidy

# Run the test
go run test_set_role.go
```

## Expected Output

You should see output like:
```
=== Test 1: Basic Session Functionality ===
Basic exec works

=== Test 2: SET ROLE Functionality ===
Available roles:
  - default
Successfully set role to 'default'

=== Test 3: Session State Persistence ===
Set max_memory_usage = 1000000
Setting persists: max_memory_usage = 1000000

=== Test 4: Multiple Operations in Same Session ===
Created test table
Inserted test data using session
Query results:
  - ID: 0, Name: test_item_0, Created: 2025-07-13T...
  - ID: 1, Name: test_item_1, Created: 2025-07-13T...
  - ID: 2, Name: test_item_2, Created: 2025-07-13T...
Cleaned up test table

=== Test 5: Error Handling ===
Correctly got error for closed session: clickhouse: session is closed

=== Test Complete ===
If you see success messages, the session functionality is working correctly!
```

## Troubleshooting

- **Connection failed**: Check if ClickHouse is running and accessible
- **Permission denied**: Make sure your user has appropriate permissions
- **Role not found**: The test tries to set "default" role - modify if needed
- **Debug logs**: Set `Debug: false` to reduce log output

## What This Tests

1. **Session Acquisition**: Getting a stateful connection
2. **SET ROLE**: Setting and maintaining role state
3. **State Persistence**: Settings that persist across queries
4. **Batch Operations**: Using sessions with batch inserts
5. **Error Handling**: Proper behavior when session is closed 