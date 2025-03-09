package parser

import "testing"

func TestParse(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		comType string
		args    []string
		err     bool
	}{
		{
			name:    "SET command",
			input:   "SET key value",
			comType: CommandSet,
			args:    []string{"key", "value"},
			err:     false,
		},
		{
			name:    "GET command",
			input:   "GET key",
			comType: CommandGet,
			args:    []string{"key"},
			err:     false,
		},
		{
			name:    "DEL command",
			input:   "DEL key",
			comType: CommandDel,
			args:    []string{"key"},
			err:     false,
		},
		{
			name:  "unknown name",
			input: "unknown key",
			err:   true,
		},
		{
			name:  "SET with invalid arguments",
			input: "SET key",
			err:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser()
			cmd, errs := p.Parse(tt.input)

			if tt.err {
				if errs == nil {
					t.Errorf("Parse() return error")
				}
				return
			}

			if errs != nil {
				t.Errorf("Parse() return error: %v", errs)
				return
			}

			if cmd.Type != tt.comType {
				t.Errorf("Parse() type = %v, want %v", cmd.Type, tt.comType)
			}

			if len(cmd.Arguments) != len(tt.args) {
				t.Errorf("Parse() arguments = %d, want %d",
					len(cmd.Arguments), len(tt.args))
				return
			}

			for i, arg := range tt.args {
				if cmd.Arguments[i] != arg {
					t.Errorf("Parse() argument[%d] = %v, want %v",
						i, cmd.Arguments[i], arg)
				}
			}
		})
	}
}
