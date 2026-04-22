package migrations

import "testing"

func TestSlugifyTable(t *testing.T) {
	cases := map[string]string{
		"users":             "users",
		"my.db.users":       "my_db_users",
		"Weird Table Name!": "weird_table_name",
		"":                  "",
		"___":               "",
	}
	for in, want := range cases {
		if got := SlugifyTable(in); got != want {
			t.Errorf("SlugifyTable(%q) = %q, want %q", in, got, want)
		}
	}
}
