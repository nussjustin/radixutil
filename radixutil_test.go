package radixutil_test

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"strings"
	"testing"
	"time"

	"github.com/nussjustin/radixutil"
)

func TestDuration(t *testing.T) {
	t.Run("MarshalRESP", func(t *testing.T) {
		tests := []struct {
			name     string
			duration radixutil.Duration
			expected string
		}{
			{
				name: "default",
				duration: radixutil.Duration{
					Duration: 5000 * time.Millisecond,
				},
				expected: ":5000\r\n",
			},
			{
				name: "custom precision",
				duration: radixutil.Duration{
					Duration:  5000 * time.Millisecond,
					Precision: time.Second,
				},
				expected: ":5\r\n",
			},
			{
				name: "custom precision multiple",
				duration: radixutil.Duration{
					Duration:  3000 * time.Millisecond,
					Precision: 1500 * time.Millisecond,
				},
				expected: ":2\r\n",
			},
			{
				name: "below precision",
				duration: radixutil.Duration{
					Duration:  500 * time.Millisecond,
					Precision: time.Second,
				},
				expected: ":0\r\n",
			},
			{
				name: "fraction",
				duration: radixutil.Duration{
					Duration:  5500 * time.Millisecond,
					Precision: time.Second,
				},
				expected: ":5\r\n",
			},
		}

		for _, test := range tests {
			test := test
			t.Run(test.name, func(t *testing.T) {
				var buf bytes.Buffer
				if err := test.duration.MarshalRESP(&buf); err != nil {
					t.Fatalf("failed to marshal duration: %s", err)
				}

				if got, want := buf.String(), test.expected; got != want {
					t.Errorf("expected %q, got %q", want, got)
				}
			})
		}
	})

	t.Run("UnmarshalRESP", func(t *testing.T) {
		tests := []struct {
			name          string
			input         string
			precision     time.Duration
			expected      time.Duration
			expectedError error
		}{
			{
				name:     "default",
				input:    ":5000\r\n",
				expected: 5000 * time.Millisecond,
			},
			{
				name:      "custom precision",
				input:     ":10\r\n",
				precision: time.Second,
				expected:  10 * time.Second,
			},
			{
				name:      "custom precision  multiple",
				input:     ":15\r\n",
				precision: 100 * time.Millisecond,
				expected:  1500 * time.Millisecond,
			},
			{
				name:     "negative value",
				input:    ":-123\r\n\r\n",
				expected: -123 * time.Millisecond,
			},
			{
				name:          "invalid type",
				input:         "$0\r\n\r\n",
				expectedError: errors.New(`expected prefix ":", got "$"`),
			},
		}

		for _, test := range tests {
			test := test
			t.Run(test.name, func(t *testing.T) {
				br := bufio.NewReader(strings.NewReader(test.input))

				var d radixutil.Duration
				d.Precision = test.precision

				err := d.UnmarshalRESP(br)
				if got, want := fmt.Sprint(err), fmt.Sprint(test.expectedError); got != want {
					t.Fatalf("unexpected error. expected %q, got %q", want, got)
				}

				if got, want := d.Duration, test.expected; got != want {
					t.Errorf("expected %q, got %q", want, got)
				}
			})
		}
	})
}

func TestUnmarshalArray(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		br := bufio.NewReader(strings.NewReader("*0\r\n"))

		if err := radixutil.UnmarshalArray(br); err != nil {
			t.Fatalf("unexpected error. expected %q, got %q", (error)(nil), err)
		}

		if got, _ := ioutil.ReadAll(br); string(got) != "" {
			t.Fatalf("unexpected data left in reader. expected %q, got %q", "", string(got))
		}
	})

	t.Run("one", func(t *testing.T) {
		br := bufio.NewReader(strings.NewReader("*1\r\n:123\r\n"))

		var want = 123
		var got int

		if err := radixutil.UnmarshalArray(br, &got); err != nil {
			t.Fatalf("unexpected error. expected %q, got %q", (error)(nil), err)
		} else if got != want {
			t.Errorf("expected %q, got %q", want, got)
		}
	})

	t.Run("many", func(t *testing.T) {
		br := bufio.NewReader(strings.NewReader("*3\r\n:123\r\n$5\r\nhello\r\n:1\r\n"))

		var want1 = 123
		var want2 = "hello"
		var want3 = 1

		var got1 int
		var got2 string
		var got3 int

		if err := radixutil.UnmarshalArray(br, &got1, &got2, &got3); err != nil {
			t.Fatalf("unexpected error. expected %q, got %q", (error)(nil), err)
		}

		if got1 != want1 {
			t.Errorf("expected %q, got %q", want1, got1)
		}

		if got2 != want2 {
			t.Errorf("expected %q, got %q", want2, got2)
		}

		if got3 != want3 {
			t.Errorf("expected %q, got %q", want3, got3)
		}
	})

	t.Run("wrong type", func(t *testing.T) {
		br := bufio.NewReader(strings.NewReader("$0\r\n"))

		got, want := radixutil.UnmarshalArray(br), errors.New(`expected prefix "*", got "$"`)
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("unexpected error. expected %q, got %q", want, got)
		}
	})

	t.Run("wrong target type", func(t *testing.T) {
		br := bufio.NewReader(strings.NewReader("*1\r\n*0\r\n"))

		var target int

		got, want := radixutil.UnmarshalArray(br, &target), errors.New(`cannot decode redis array into int`)
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("unexpected error. expected %q, got %q", want, got)
		}
	})

	t.Run("wrong number of elements", func(t *testing.T) {
		br := bufio.NewReader(strings.NewReader("*2\r\n*0\r\n*0\r\n"))

		var v interface{}

		got, want := radixutil.UnmarshalArray(br, &v), errors.New(`expected array of length 1, got 2`)
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("unexpected error. expected %q, got %q", want, got)
		}
	})

	t.Run("not pointer", func(t *testing.T) {
		br := bufio.NewReader(strings.NewReader("*1\r\n*0\r\n"))

		got, want := radixutil.UnmarshalArray(br, 0), errors.New(`can't unmarshal into int`)
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("unexpected error. expected %q, got %q", want, got)
		}
	})
}
