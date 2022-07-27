package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/affix-io/affix/version"
	"github.com/affix-io/ioes"
	"github.com/spf13/cobra"
)

// NewVersionCommand creates a new `affix version` cobra command that prints the current affix version
func NewVersionCommand(_ Factory, ioStreams ioes.IOStreams) *cobra.Command {
	var format string

	cmd := &cobra.Command{
		Use:   "version",
		Short: "print the version number",
		Long: `affix uses semantic versioning.

For updates & further information check https://github.com/affix-io/affix/releases`,
		Annotations: map[string]string{
			"group": "other",
		},
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			switch format {
			case "json":
				data, err := json.Marshal(version.Map())
				if err != nil {
					return err
				}
				printInfo(ioStreams.Out, string(data))
			case "pretty":
				printInfo(ioStreams.Out, version.Summary())
			default:
				return fmt.Errorf("unrecognized output format: %q", format)
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&format, "format", "pretty", "output format. One of (pretty|json)")
	return cmd
}
