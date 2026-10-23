// Inside a Surfra job or app: no credential or URL to set. Writes go.txt and reads it back.
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/surfra-io/surfra-go"
)

func main() {
	ctx := context.Background()
	weights := surfra.NewVolume("weights")
	if _, err := weights.WriteFile(ctx, "from/go.txt", []byte("go")); err != nil {
		log.Fatal(err)
	}
	back, err := weights.ReadFile(ctx, "from/go.txt")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("go wrote and read %d bytes\n", len(back))
}
