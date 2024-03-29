package docgenerator

import (
	"Acrux/internal/template"
	"fmt"
	"strings"
)

type DocumentType string

const (
	JsonDocument   string = "json"
	BinaryDocument string = "binary"
)

const DefaultDocSize int = 128
const DefaultKeySize int = 250

// Generator helps to generate random document for inserting and updating random
// as per the doc loading task requirement.
type Generator struct {
	KeySize   int               `json:"keySize"`
	DocType   string            `json:"docType"`
	KeyPrefix string            `json:"keyPrefix"`
	KeySuffix string            `json:"keySuffix"`
	Seed      int64             `json:"seed"`
	SeedEnd   int64             `json:"seedEnd"`
	Template  template.Template `json:"template"`
}

func ConfigGenerator(doctype, keyPrefix, keySuffix string, keySize int, seed, seedEnd int64,
	template template.Template) *Generator {

	return &Generator{
		KeySize:   keySize,
		DocType:   doctype,
		KeyPrefix: keyPrefix,
		KeySuffix: keySuffix,
		Seed:      seed,
		SeedEnd:   seedEnd,
		Template:  template,
	}
}

type QueryGenerator struct {
	Template template.Template
}

func ConfigQueryGenerator(template template.Template) *QueryGenerator {
	return &QueryGenerator{
		Template: template,
	}
}

// GetDocIdAndKey will return key for the next document
func (g *Generator) GetDocIdAndKey(iteration int64) (string, int64) {
	newKey := iteration + g.Seed
	return fmt.Sprintf("%s%d%s", g.KeyPrefix, newKey, g.KeySuffix), newKey
}

// BuildKey returns the formatted key with unique identifier.
func (g *Generator) BuildKey(key int64) string {
	tempKey := fmt.Sprintf("%s%d%s", g.KeyPrefix, key, g.KeySuffix)
	if g.KeySize >= 0 && len(tempKey) < g.KeySize {
		tempKey += strings.Repeat("a", g.KeySize-len(tempKey))
	}
	return tempKey
}
