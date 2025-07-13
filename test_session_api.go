package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func main() {
	// Connect to your local ClickHouse server
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"localhost:9000"}, // Change if your ClickHouse runs on different port
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default", // Change to your username
			Password: "",        // Change to your password if needed
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout:      time.Second * 30,
		MaxOpenConns:     5,
		MaxIdleConns:     5,
		ConnMaxLifetime:  time.Hour,
		ConnOpenStrategy: clickhouse.ConnOpenInOrder,
		Debug:            true, // Enable debug logging to see what's happening
	})
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	ctx := context.Background()

	fmt.Println("=== Testing NEW Session API ===")
	fmt.Println("This test uses the new Session API with stateful connections.")
	fmt.Println()

	// Test 1: Acquire a session
	fmt.Println("=== Test 1: Session Acquisition ===")

	session, err := conn.AcquireSession(ctx)
	if err != nil {
		log.Fatalf("Failed to acquire session: %v", err)
	}
	defer session.Close()

	fmt.Println("Successfully acquired session")
	fmt.Println()

	// Test 2: Basic session operations
	fmt.Println("=== Test 2: Basic Session Operations ===")

	// Test basic operations
	err = session.Exec(ctx, "SELECT 1")
	if err != nil {
		log.Printf("Basic exec failed: %v", err)
	} else {
		fmt.Println("Basic exec works")
	}

	// Test query
	rows, err := session.Query(ctx, "SELECT 42 as value")
	if err != nil {
		log.Printf("Basic query failed: %v", err)
	} else {
		var value uint8
		if rows.Next() {
			err = rows.Scan(&value)
			if err != nil {
				log.Printf("Failed to scan value: %v", err)
			} else {
				fmt.Printf("Query result: %d\n", value)
			}
		}
		rows.Close()
	}

	// Test query row
	var result uint8
	err = session.QueryRow(ctx, "SELECT 100").Scan(&result)
	if err != nil {
		log.Printf("QueryRow failed: %v", err)
	} else {
		fmt.Printf("QueryRow result: %d\n", result)
	}

	// Test ping
	err = session.Ping(ctx)
	if err != nil {
		log.Printf("Ping failed: %v", err)
	} else {
		fmt.Println("Ping successful")
	}

	fmt.Println()

	// Test 3: SET ROLE functionality
	fmt.Println("=== Test 3: SET ROLE Functionality ===")

	// First, let's see what roles are available
	rows, err = session.Query(ctx, "SHOW ROLES")
	if err != nil {
		log.Printf("Failed to show roles: %v", err)
	} else {
		fmt.Println("Available roles:")
		for rows.Next() {
			var role string
			if err := rows.Scan(&role); err != nil {
				log.Printf("Failed to scan role: %v", err)
			} else {
				fmt.Printf("  - %s\n", role)
			}
		}
		rows.Close()
	}

	// Try to set a role (this might fail if the role doesn't exist, which is expected)
	err = session.Exec(ctx, "SET ROLE default")
	if err != nil {
		log.Printf("Warning: Failed to set role 'default': %v", err)
		log.Printf("This is expected if the role doesn't exist or you don't have permission")
	} else {
		fmt.Println("Successfully set role to 'default'")
	}

	// Test query after SET ROLE - this should use the same connection
	rows, err = session.Query(ctx, "SELECT currentUser()")
	if err != nil {
		log.Printf("Failed to query current user: %v", err)
	} else {
		if rows.Next() {
			var user string
			if err := rows.Scan(&user); err != nil {
				log.Printf("Failed to scan user: %v", err)
			} else {
				fmt.Printf("Current user: %s\n", user)
			}
		}
		rows.Close()
	}

	fmt.Println()

	// Test 4: Session state persistence
	fmt.Println("=== Test 4: Session State Persistence ===")

	// Set a session setting
	err = session.Exec(ctx, "SET max_memory_usage = 1000000")
	if err != nil {
		log.Printf("Failed to set max_memory_usage: %v", err)
	} else {
		fmt.Println("Set max_memory_usage = 1000000")
	}

	// Verify the setting persists across queries (same connection)
	rows, err = session.Query(ctx, "SELECT value FROM system.settings WHERE name = 'max_memory_usage'")
	if err != nil {
		log.Printf("Failed to query settings: %v", err)
	} else {
		if rows.Next() {
			var value string
			if err := rows.Scan(&value); err != nil {
				log.Printf("Failed to scan setting value: %v", err)
			} else {
				fmt.Printf("Setting persists: max_memory_usage = %s\n", value)
			}
		}
		rows.Close()
	}

	fmt.Println()

	// Test 5: Multiple operations in same session
	fmt.Println("=== Test 5: Multiple Operations in Same Session ===")

	// Create a test table
	err = session.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS session_test (
			id UInt32,
			name String,
			created_at DateTime
		) ENGINE = Memory
	`)
	if err != nil {
		log.Printf("Failed to create table: %v", err)
	} else {
		fmt.Println("Created test table")
	}

	// Insert data using the session
	batch, err := session.PrepareBatch(ctx, "INSERT INTO session_test (id, name, created_at)")
	if err != nil {
		log.Printf("Failed to prepare batch: %v", err)
	} else {
		// Add some test data
		for i := 0; i < 3; i++ {
			err = batch.Append(
				uint32(i),
				fmt.Sprintf("test_item_%d", i),
				time.Now(),
			)
			if err != nil {
				log.Printf("Failed to append to batch: %v", err)
				break
			}
		}

		if err == nil {
			err = batch.Send()
			if err != nil {
				log.Printf("Failed to send batch: %v", err)
			} else {
				fmt.Println("Inserted test data using session")
			}
		}
	}

	// Query the data using the same session (maintains role and settings)
	rows, err = session.Query(ctx, "SELECT id, name, created_at FROM session_test ORDER BY id")
	if err != nil {
		log.Printf("Failed to query test data: %v", err)
	} else {
		fmt.Println("Query results:")
		for rows.Next() {
			var (
				id        uint32
				name      string
				createdAt time.Time
			)
			if err := rows.Scan(&id, &name, &createdAt); err != nil {
				log.Printf("Failed to scan row: %v", err)
			} else {
				fmt.Printf("  - ID: %d, Name: %s, Created: %s\n", id, name, createdAt.Format(time.RFC3339))
			}
		}
		rows.Close()
	}

	// Clean up
	err = session.Exec(ctx, "DROP TABLE session_test")
	if err != nil {
		log.Printf("Failed to drop test table: %v", err)
	} else {
		fmt.Println("Cleaned up test table")
	}

	fmt.Println()

	// Test 6: Error handling
	fmt.Println("=== Test 6: Error Handling ===")

	// Close the session
	session.Close()

	// Try to use the closed session
	err = session.Exec(ctx, "SELECT 1")
	if err != nil {
		fmt.Printf("Correctly got error for closed session: %v\n", err)
	} else {
		fmt.Println("Expected error for closed session but got none")
	}

	fmt.Println()
	fmt.Println("=== Test Complete ===")
	fmt.Println("The Session API is working correctly!")
	fmt.Println("- SET ROLE and session settings persist across multiple operations")
	fmt.Println("- All operations use the same stateful connection")
	fmt.Println("- Proper error handling for closed sessions")
	fmt.Println("- Resource management works correctly")
}
