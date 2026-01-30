#!/usr/bin/perl

use strict;
use warnings;
use File::Copy qw(copy);

# Usage:
#   perl transform_ora2pg_insert_inplace_heuristic.pl yourfile.sql
#
# The script makes a backup yourfile.sql.bak before overwriting.

my $file = shift or die "Usage: $0 <ora2pg_insert_output.sql>\n";

# create backup
my $bak = "$file.bak";
copy($file, $bak) or die "Failed to create backup $bak: $!";

open my $fh, '<', $file or die "Cannot open $file: $!";
my @lines = <$fh>;
close $fh;

# Transform a single "complex" field string like:
#   '{AI}'
#   '{Tech,Database,Cloud}'
#   '("{456 Oak Ave,Los Angeles,90210,USA}")'
# into:
#   {AI}                        (array)
#   {Tech,Database,Cloud}       (array)
#   ("456 Oak Ave","Los Angeles",90210,"USA") (composite)
#
# Returns undef if it doesn't recognize the pattern.
sub transform_complex_field {
    my ($col) = @_;

    my $original = $col;

    # Trim outer whitespace
    $col =~ s/^\s+|\s+$//g;

    my $inner;
    my $kind = ''; # 'object' or 'array' or ''

    # Pattern 1: '("{...}")' - composite type with curly braces wrapper
    if ($col =~ /^\(\"\{(.*)\}\"\)$/s) {
        # ("{456 Oak Ave,Los Angeles,90210,USA}")
        $inner = $1;
        $kind  = 'object';
    }
    # Pattern 2: '{...}' - simple array
    elsif ($col =~ /^\{(.*)\}$/s) {
        # {AI} or {Tech,Database,Cloud}
        $inner = $1;
        $kind  = 'array';
    }
    # Pattern 3: ({"(a,b,c)"}) - composite with parens and quotes
    elsif ($col =~ /^\(\{\"?\((.*)\)\"?\}\)$/s) {
        # ({"(a,b,c)"})
        $inner = $1;
        $kind  = 'object';
    }
    # Pattern 4: ("{1,2,3}") or ("{a,b,c}") - quoted array
    elsif ($col =~ /^\(\"?\{(.*)\}\"?\)$/s) {
        # ("{1,2,3}") or ("{a,b,c}")
        $inner = $1;
        $kind  = 'array';
    }
    # Pattern 5: Plain parentheses (a,b,c)
    elsif ($col =~ /^\((.*)\)$/s) {
        $inner = $1;
        $kind  = 'object';
    }
    # Pattern 6: "a,b,c" – treat as CSV inside a single string
    elsif ($col =~ /^\"(.*)\"$/s && $col =~ /,/) {
        $inner = $1;
        # Decide based on content
    } else {
        # Not a complex-looking field; leave unchanged
        return undef;
    }

    return undef unless defined $inner;

    # Smart CSV parsing that handles quoted values
    my @parts;
    my $current = '';
    my $in_quotes = 0;
    
    for my $char (split //, $inner) {
        if ($char eq '"' && ($current eq '' || substr($current, -1) ne '\\')) {
            $in_quotes = !$in_quotes;
            $current .= $char;
        } elsif ($char eq ',' && !$in_quotes) {
            push @parts, $current;
            $current = '';
        } else {
            $current .= $char;
        }
    }
    push @parts, $current if defined $current;

    # Trim whitespace for each part and remove quotes if present
    for (@parts) {
        s/^\s+|\s+$//g;
        # Remove outer quotes if they exist
        s/^"(.*)"$/$1/;
    }

    # Decide type if still unknown
    if (!$kind) {
        my $has_empty   = grep { $_ eq '' } @parts;
        my $has_numeric = grep { $_ =~ /^-?\d+(?:\.\d+)?$/ } @parts;

        if ($has_empty || $has_numeric) {
            $kind = 'object';  # composite/object
        } else {
            $kind = 'array';   # string list
        }
    }

    if ($kind eq 'array') {
        # Build array literal {a,b,c}
        # Array elements are not double-quoted in PostgreSQL array syntax
        my $new = '{' . join(',', @parts) . '}';
        return $new;
    }
    else {
        # Composite/object: numeric parts unquoted, empty => NULL slot,
        # strings in double quotes.
        for my $p (@parts) {
            if ($p eq '') {
                # empty: leave as empty between commas in composite
                $p = '';
            }
            elsif ($p =~ /^-?\d+(?:\.\d+)?$/) {
                # numeric: keep as-is
                $p = $p;
            }
            else {
                # string: wrap in double quotes, escape inner quotes and backslashes
                $p =~ s/\\/\\\\/g;  # escape backslash first
                $p =~ s/"/\\"/g;    # then escape quotes
                $p = qq{"$p"};
            }
        }
        my $new = '(' . join(',', @parts) . ')';
        return $new;
    }
}

# Process each line:
# We do NOT rely on valid SQL '...' parsing (because of cases like O'Reilly).
# Instead we:
#   - find single-quoted chunks whose CONTENT clearly looks like our
#     complex wrappers: ({"( ... )"}), ("{ ... }"), { ... }
#   - transform just those chunks.
for my $line (@lines) {

    $line =~ s{
        (E?)'                            # $1: optional E prefix
        (                                # $2: body that looks like a complex wrapper
           \(\{\"?\( .*? \)\"?\}\)       # ({"( ... )"})
          |\"?\{ .*? \}\"?               # "{ ... }" or { ... } inside the string
          |\(\"?\{ .*? \}\"?\)           # ("{ ... }")
        )
        '                                # closing single quote
    }{
        my $prefix  = $1;
        my $body    = $2;

        # Normalize any doubled single quotes inside this body back to literal '
        # (just in case ora2pg had escaped them already in some cases)
        my $decoded = $body;
        $decoded =~ s/''/'/g;

        my $transformed = transform_complex_field($decoded);

        my $final = defined($transformed) ? $transformed : $decoded;

        # Re-escape single quotes for SQL
        $final =~ s/'/''/g;

        $prefix . "'" . $final . "'";
    }gsex;

}

# Write back to same file (in-place)
open my $out, '>', $file or die "Cannot write to $file: $!";
print $out @lines;
close $out;

print "Fixed.\n";