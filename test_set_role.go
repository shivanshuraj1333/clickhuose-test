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

	fmt.Println("=== ClickHouse SET ROLE Test ===")
	fmt.Println("This test demonstrates the current behavior and what the new Session API would provide.")
	fmt.Println()

	// Test 1: Current behavior (without sessions)
	fmt.Println("=== Test 1: Current Behavior (No Session API) ===")

	// Try to set a role using the current API
	err = conn.Exec(ctx, "SET ROLE default")
	if err != nil {
		log.Printf("Failed to set role: %v", err)
	} else {
		fmt.Println("Successfully executed 'SET ROLE default'")
	}

	// Try to verify the role was set
	rows, err := conn.Query(ctx, "SELECT currentUser()")
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

	// Test 2: Session state persistence (simulated)
	fmt.Println("\n=== Test 2: Session State Persistence (Simulated) ===")

	// Set a session setting
	err = conn.Exec(ctx, "SET max_memory_usage = 1000000")
	if err != nil {
		log.Printf("Failed to set max_memory_usage: %v", err)
	} else {
		fmt.Println("Set max_memory_usage = 1000000")
	}

	// Verify the setting (this might not persist due to connection pooling)
	rows, err = conn.Query(ctx, "SELECT value FROM system.settings WHERE name = 'max_memory_usage'")
	if err != nil {
		log.Printf("Failed to query settings: %v", err)
	} else {
		if rows.Next() {
			var value string
			if err := rows.Scan(&value); err != nil {
				log.Printf("Failed to scan setting value: %v", err)
			} else {
				fmt.Printf("Current setting: max_memory_usage = %s\n", value)
			}
		}
		rows.Close()
	}

	// Test 3: Multiple operations (current behavior)
	fmt.Println("\n=== Test 3: Multiple Operations (Current Behavior) ===")

	// Create a test table
	err = conn.Exec(ctx, `
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

	// Insert data
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO session_test (id, name, created_at)")
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
				fmt.Println("Inserted test data")
			}
		}
	}

	// Query the data
	rows, err = conn.Query(ctx, "SELECT id, name, created_at FROM session_test ORDER BY id")
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
	err = conn.Exec(ctx, "DROP TABLE session_test")
	if err != nil {
		log.Printf("Failed to drop test table: %v", err)
	} else {
		fmt.Println("Cleaned up test table")
	}

	// Test 4: What the new Session API would provide
	fmt.Println("\n=== Test 4: What the New Session API Would Provide ===")
	fmt.Println("With the new Session API, you would be able to:")
	fmt.Println("1. Acquire a session: session, err := conn.AcquireSession(ctx)")
	fmt.Println("2. Set role in session: session.Exec(ctx, \"SET ROLE admin\")")
	fmt.Println("3. Execute multiple queries with the same role:")
	fmt.Println("   - session.Query(ctx, \"SELECT currentUser()\")")
	fmt.Println("   - session.Query(ctx, \"SELECT * FROM table\")")
	fmt.Println("4. All queries would use the same connection, maintaining state")
	fmt.Println("5. Session cleanup: session.Close()")

	fmt.Println("\n=== Current Limitations ===")
	fmt.Println("No way to maintain connection state across multiple operations")
	fmt.Println("SET ROLE doesn't persist to subsequent queries due to connection pooling")
	fmt.Println("Session settings may not persist between operations")
	fmt.Println("No way to acquire a stateful connection for multiple operations")

	fmt.Println("\n=== Proposed Solution ===")
	fmt.Println("New Session API would provide stateful connections")
	fmt.Println("SET ROLE would persist across all session operations")
	fmt.Println("Session settings would be maintained")
	fmt.Println("Proper resource management with session.Close()")

	fmt.Println("\n=== Test Complete ===")
	fmt.Println("This demonstrates the current behavior and the need for the Session API!")
}
