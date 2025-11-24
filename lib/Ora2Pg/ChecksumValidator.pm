package Ora2Pg::ChecksumValidator;
#------------------------------------------------------------------------------
# Project  : Oracle to PostgreSQL database schema converter
# Name     : ChecksumValidator.pm
# Language : Perl
# Authors  : Gilles Darold, gilles _AT_ darold _DOT_ net
# Copyright: Copyright (c) 2000-2025 : Gilles Darold - All rights reserved -
# Function : Checksum-based validation for complex Oracle data types
# Usage    : Used internally by Ora2Pg to validate data integrity
#------------------------------------------------------------------------------

use strict;
use warnings;
use Digest::SHA qw(sha256_hex);
use JSON::PP;
use Carp qw(confess);

our $VERSION = '1.0';

=head1 NAME

Ora2Pg::ChecksumValidator - Checksum-based validation for complex Oracle data types

=head1 DESCRIPTION

This module provides checksum-based validation functionality for complex Oracle
data types (ARRAY, MAP, RECORD) during Oracle to PostgreSQL migration.

=cut

sub new {
    my ($class, %options) = @_;
    
    my $self = {
        # Configuration options
        enabled               => $options{enabled} || 0,
        sample_size          => $options{sample_size} || 1000,
        sample_percentage    => $options{sample_percentage} || 5,
        hash_algorithm       => $options{hash_algorithm} || 'SHA256',
        validation_output    => $options{validation_output} || 'validation_report.txt',
        checksum_file        => $options{checksum_file} || 'checksums.json',
        
        # Internal state
        checksums            => {},
        validation_errors    => [],
        tables_processed     => 0,
        rows_validated       => 0,
        complex_types_found  => {},
        row_normalization_debug => {},
    };
    
    bless $self, $class;
    return $self;
}

=head2 is_complex_type

Check if a data type is one of the supported complex types

=cut

sub is_complex_type {
    my ($self, $src_type) = @_;
    
    return 0 unless defined $src_type;
    
    # Check for Oracle complex types
    return 1 if $src_type =~ /^(VARRAY|VARYING\s+ARRAY)/i;
    return 1 if $src_type =~ /^TABLE\s+OF/i;
    return 1 if $src_type =~ /^OBJECT\s+TYPE/i;
    return 1 if $src_type =~ /^NESTED\s+TABLE/i;
    return 1 if $src_type =~ /^ARRAY\s*\(/i;
    return 1 if $src_type =~ /^MAP\s*\(/i;
    return 1 if $src_type =~ /^RECORD\s*\(/i;
    
    # Check for user-defined complex types by name patterns
    # These are common patterns for user-defined collection and object types
    return 1 if $src_type =~ /_ARRAY$/i;      # e.g., NUMBER_ARRAY, STRING_ARRAY
    return 1 if $src_type =~ /_LIST$/i;       # e.g., STRING_LIST, ID_LIST
    return 1 if $src_type =~ /_TYPE$/i;       # e.g., ADDRESS_TYPE, CUSTOMER_TYPE, ARRAY_TYPE
    return 1 if $src_type =~ /_COLLECTION$/i; # e.g., DATA_COLLECTION
    return 1 if $src_type =~ /_VARRAY$/i;     # e.g., NAMES_VARRAY
    return 1 if $src_type =~ /_TABLE$/i;      # e.g., EMPLOYEES_TABLE
    
    # Check for PostgreSQL array type
    return 1 if $src_type eq 'ARRAY_TYPE';    # PostgreSQL generic array type
    
    # Check for Oracle specific array types from export
    return 1 if $src_type eq 'STRING_LIST';   # Oracle nested table type
    return 1 if $src_type eq 'NUMBER_ARRAY';  # Oracle varray type
    
    # Check for types that contain parentheses (type definitions)
    return 1 if $src_type =~ /\([^)]+\)/;     # e.g., STRING_LIST(varchar)
    
    return 0;
}

=head2 serialize_complex_value

Serialize a complex value to a canonical string representation for hashing

=cut

sub serialize_complex_value {
    my ($self, $value, $type, $table, $row_key, $column_index) = @_;
    
    return '' unless defined $value;
    
    # Handle NULL values
    return 'NULL' if !length($value) || $value eq 'NULL';
    
    # Normalize complex types to semantic equivalence
    my $normalized_value = $self->_normalize_complex_format($value);
    
    my $serialized_value;
    
    # For Oracle arrays/varrays (format: [item1,item2,item3]) and PostgreSQL arrays
    # Match array types by pattern instead of hardcoding specific names
    if ($type =~ /^(VARRAY|VARYING\s+ARRAY|TABLE\s+OF|ARRAY)/i || 
        $type eq 'ARRAY_TYPE' || 
        $type =~ /_ARRAY$/i || 
        $type =~ /_LIST$/i || 
        $type =~ /_VARRAY$/i || 
        $type =~ /_COLLECTION$/i ||
        $type =~ /_TABLE$/i) {
        $serialized_value = $self->_serialize_array_semantic($normalized_value);
    }
    # For Oracle object types/records (format: {key1:value1,key2:value2})
    elsif ($type =~ /^(OBJECT\s+TYPE|RECORD)/i) {
        $serialized_value = $self->_serialize_record_semantic($normalized_value);
    }
    # For custom object types (e.g., ADDRESS_TYPE, PERSON_TYPE, etc.)
    elsif ($type =~ /^[A-Z][A-Z0-9_]*_TYPE$/i || $value =~ /^[a-zA-Z_][a-zA-Z0-9_]*\s*\(/i) {
        $serialized_value = $self->_serialize_record_semantic($normalized_value);
    }
    # For maps (format: {"key1":value1,"key2":value2})
    elsif ($type =~ /^MAP/i) {
        $serialized_value = $self->_serialize_map($normalized_value);
    }
    # Fallback for other types
    else {
        $serialized_value = $normalized_value;
    }
    
    # Collect debug information for normalization report
    if (defined $table && defined $row_key && defined $column_index && ($value ne $normalized_value || $value ne $serialized_value)) {
        $self->{row_normalization_debug}{$table}{$row_key} ||= [];
        push @{$self->{row_normalization_debug}{$table}{$row_key}}, {
            column_index => $column_index,
            src_type => $type,
            original_value => $value,
            normalized_value => $normalized_value,
            serialized_value => $serialized_value
        };
    }
    
    return $serialized_value;
}

=head2 _serialize_array

Serialize an array value to canonical form

=cut

sub _serialize_array {
    my ($self, $value) = @_;
    
    # Remove whitespace and normalize
    $value =~ s/^\s*\[\s*//;
    $value =~ s/\s*\]\s*$//;
    
    # Split elements and sort for consistency
    my @elements = split(/\s*,\s*/, $value);
    @elements = map { $_ // 'NULL' } @elements;
    
    # Return canonical JSON array
    return JSON::PP->new->canonical(1)->encode(\@elements);
}

=head2 _normalize_complex_format

Normalize different complex type formats to a standard representation
Handles Oracle malformed patterns and PostgreSQL native formats

=cut

sub _normalize_complex_format {
    my ($self, $value) = @_;
    
    return $value unless defined $value;
    
    # Debug: uncomment for testing
    # if ($value =~ /10,20,30/) {
    #     print "DEBUG: Normalizing '$value' (length: " . length($value) . ")\n";
    #     use Data::Dumper;
    #     print "DEBUG: Raw bytes: " . join(",", map { ord($_) } split //, $value) . "\n";
    # }
    
    # Oracle malformed pattern: ({"(content)"}) - let type-specific methods handle formatting
    if ($value =~ /^\(\{"?\(([^)]+)\)"?\}\)$/) {
        my $content = $1;
        # Return the extracted content for type-specific processing
        return $content;
    }
    
    # PostgreSQL array format: {element1,element2,element3} - already normalized
    if ($value =~ /^\{[^}]*\}$/) {
        return $value;
    }
    
    # Oracle numeric array pattern: ("{1,2,3}") - must come before generic composite pattern
    if ($value =~ /^\(\"(\{[^}]+\})\"\)$/) {
        return $1;
    }
    
    # Alternative Oracle array format without outer parentheses: "{1,2,3}"
    if ($value =~ /^\"(\{[^}]+\})\"$/) {
        return $1;
    }
    
    # PostgreSQL composite format: ("field1","field2",field3) - already normalized
    if ($value =~ /^\([^)]*\)$/) {
        return $value;
    }
    
    return $value;
}

=head2 _serialize_array_semantic

Serialize array to semantic canonical form (order-independent for sets)

=cut

sub _serialize_array_semantic {
    my ($self, $value) = @_;
    
    # Extract elements from various formats
    my @elements = ();
    
    if ($value =~ /^\{([^}]*)\}$/) {
        # PostgreSQL array format: {elem1,elem2,elem3}
        my $content = $1;
        @elements = split(/,/, $content) if $content;
    } elsif ($value =~ /^\[([^]]*)\]$/) {
        # Bracket array format: [elem1,elem2,elem3]
        my $content = $1;
        @elements = split(/,/, $content) if $content;
    } elsif ($value =~ /^"\{([^}]*)\}"$/) {
        # Quoted array format: "{elem1,elem2,elem3}"
        my $content = $1;
        @elements = split(/,/, $content) if $content;
    } else {
        # Fallback: assume comma-separated (already normalized by _normalize_complex_format)
        @elements = split(/,/, $value);
    }
    
    # Clean and normalize elements
    @elements = map { 
        s/^\s*|\s*$//g;  # trim whitespace
        s/^"(.*)"$/$1/;  # remove surrounding quotes
        $_ || 'NULL'
    } @elements;
    
    # Sort for deterministic comparison (assuming arrays are sets)
    # For ordered arrays, remove this sort
    @elements = sort @elements;
    
    # Return canonical representation
    return 'ARRAY[' . join(',', map { '"' . $_ . '"' } @elements) . ']';
}

=head2 _serialize_record_semantic

Serialize composite/record to semantic canonical form

=cut

sub _serialize_record_semantic {
    my ($self, $value) = @_;
    
    my @fields = ();
    
    # Handle object type format: TYPENAME(field1,field2,field3) or typename(field1,field2,field3)
    if ($value =~ /^[a-zA-Z_][a-zA-Z0-9_]*\s*\(([^)]*)\)$/i) {
        # Object type format - extract content and ignore type name for semantic equivalence
        my $content = $1;
        @fields = $self->_parse_composite_fields($content);
    } elsif ($value =~ /^\(([^)]*)\)$/) {
        # PostgreSQL composite format: ("field1","field2",123,"field4")
        my $content = $1;
        @fields = $self->_parse_composite_fields($content);
    } elsif ($value =~ /^\{([^}]*)\}$/) {
        # Object-style format: {field1,field2,field3}
        my $content = $1;
        @fields = split(/,/, $content, -1);  # -1 preserves trailing empty fields
    } else {
        # Fallback
        @fields = split(/,/, $value, -1);  # -1 preserves trailing empty fields
    }
    
    # Clean and normalize fields
    @fields = map { 
        s/^\s*|\s*$//g;         # trim whitespace
        s/^"(.*)"$/$1/;         # remove surrounding double quotes
        s/^'(.*)'$/$1/;         # remove surrounding single quotes
        # Re-quote strings (leave numbers unquoted)
        /^\d+(\.\d+)?$/ ? $_ : '"' . $_ . '"'
    } @fields;
    
    # Return canonical representation (preserves field order)
    return 'RECORD(' . join(',', @fields) . ')';
}

=head2 _parse_composite_fields

Parse PostgreSQL composite type fields, handling quoted strings and numbers

=cut

sub _parse_composite_fields {
    my ($self, $content) = @_;
    
    my @fields = ();
    my $current_field = '';
    my $in_quotes = 0;
    my $escape_next = 0;
    
    for my $char (split //, $content) {
        if ($escape_next) {
            $current_field .= $char;
            $escape_next = 0;
        } elsif ($char eq '\\') {
            $escape_next = 1;
            $current_field .= $char;
        } elsif ($char eq '"') {
            $in_quotes = !$in_quotes;
            $current_field .= $char;
        } elsif ($char eq ',' && !$in_quotes) {
            push @fields, $current_field;
            $current_field = '';
        } else {
            $current_field .= $char;
        }
    }
    
    # Always push the last field, even if empty (for trailing commas)
    push @fields, $current_field;
    
    return @fields;
}

=head2 _serialize_record

Serialize a record/object value to canonical form

=cut

sub _serialize_record {
    my ($self, $value) = @_;
    
    # Parse record format: {key1:value1,key2:value2}
    $value =~ s/^\s*\{\s*//;
    $value =~ s/\s*\}\s*$//;
    
    my %record = ();
    
    # Split key-value pairs
    my @pairs = split(/\s*,\s*/, $value);
    foreach my $pair (@pairs) {
        if ($pair =~ /^([^:]+):\s*(.*)$/) {
            my ($key, $val) = ($1, $2);
            $key =~ s/^\s*["']?//; $key =~ s/["']?\s*$//;  # Remove quotes
            $val =~ s/^\s*["']?//; $val =~ s/["']?\s*$//;  # Remove quotes
            $record{$key} = $val // 'NULL';
        }
    }
    
    # Return canonical JSON object
    return JSON::PP->new->canonical(1)->encode(\%record);
}

=head2 _serialize_map

Serialize a map value to canonical form

=cut

sub _serialize_map {
    my ($self, $value) = @_;
    
    # Parse map format: {"key1":value1,"key2":value2}
    # Try to decode as JSON first
    eval {
        my $decoded = JSON::PP->new->decode($value);
        if (ref $decoded eq 'HASH') {
            return JSON::PP->new->canonical(1)->encode($decoded);
        }
    };
    
    # Fallback to manual parsing if JSON decode fails
    return $self->_serialize_record($value);
}

=head2 _convert_oracle_ref_to_pg_format

Convert Oracle's Perl array/hash references to PostgreSQL string format.
Oracle DBD::Oracle returns complex types as Perl references, while PostgreSQL
stores them as text strings. This method normalizes the Oracle format to match
PostgreSQL's representation for consistent checksum generation.

=cut

sub _convert_oracle_ref_to_pg_format {
    my ($self, $value, $src_type) = @_;
    
    return $value unless defined $value;
    
    # Check if this is a Perl reference (Oracle returns ARRAY or HASH refs for complex types)
    my $ref_type = ref($value);
    
    if ($ref_type eq 'ARRAY') {
        # Determine if this is an object type (composite) or array type
        # Object types in Oracle are returned as arrays of field values
        # Array types (VARRAY, nested table) are also returned as arrays
        
        # Check array/collection types FIRST (more specific patterns)
        if ($src_type =~ /^(ARRAY_TYPE|VARRAY|TABLE\s+OF|NESTED|_ARRAY|_LIST|_VARRAY|_TABLE|_COLLECTION)$/i ||
            $src_type =~ /(STRING_LIST|NUMBER_ARRAY)/i) {
            # This is an array/collection type - convert to PostgreSQL array format: {elem1,elem2,...}
            return $self->_convert_array_to_pg_array($value);
        } 
        # Check object/composite types SECOND
        elsif ($src_type =~ /(OBJECT_TYPE|OBJECT|RECORD|ADDRESS|CUSTOMER|PERSON|COMPOSITE|_TYPE$)/i) {
            # This is a composite/object type - convert to PostgreSQL composite format: (field1,field2,...)
            return $self->_convert_array_to_composite($value);
        } 
        # Default to array format if type is ambiguous
        else {
            return $self->_convert_array_to_pg_array($value);
        }
        
    } elsif ($ref_type eq 'HASH') {
        # Oracle returns object types as Perl hash references
        # Convert to PostgreSQL composite type format: (field1,field2,field3)
        return $self->_convert_hash_to_composite($value);
        
    } elsif ($ref_type) {
        # Unknown reference type - stringify it
        return "$value";
    }
    
    # Not a reference - return as-is
    return $value;
}

=head2 _convert_array_to_composite

Convert a Perl array reference to PostgreSQL composite type format (for object types)

=cut

sub _convert_array_to_composite {
    my ($self, $array_ref) = @_;
    
    return '()' unless defined $array_ref && ref($array_ref) eq 'ARRAY';
    
    my @elements = @$array_ref;
    
    # Handle empty array
    return '()' unless @elements;
    
    # Convert values, preserving NULLs as empty fields (PostgreSQL composite format)
    my @fields = map {
        if (!defined $_) {
            ''  # Empty field for NULL in composite type
        } elsif (ref($_) eq 'HASH') {
            # Nested object
            $self->_convert_hash_to_composite($_)
        } elsif (ref($_) eq 'ARRAY') {
            # Nested array
            $self->_convert_array_to_pg_array($_)
        } else {
            # Simple value - quote if necessary
            if ($_ =~ /[,\s()]/ || $_ eq '') {
                my $escaped = $_;
                $escaped =~ s/"/\\"/g;  # Escape quotes
                qq("$escaped")
            } else {
                $_
            }
        }
    } @elements;
    
    return '(' . join(',', @fields) . ')';
}

=head2 _convert_array_to_pg_array

Convert a Perl array reference to PostgreSQL array format

=cut

sub _convert_array_to_pg_array {
    my ($self, $array_ref) = @_;
    
    return '{}' unless defined $array_ref && ref($array_ref) eq 'ARRAY';
    
    my @elements = @$array_ref;
    
    # Handle empty arrays
    return '{}' unless @elements;
    
    # Convert elements to strings, handling NULLs
    my @string_elements = map {
        if (!defined $_) {
            'NULL'  # NULL in array
        } elsif (ref($_) eq 'HASH') {
            # Nested object type within array - convert to composite format
            $self->_convert_hash_to_composite($_)
        } elsif (ref($_) eq 'ARRAY') {
            # Nested array - recursive conversion
            $self->_convert_array_to_pg_array($_)
        } else {
            # Simple value - use as-is
            $_
        }
    } @elements;
    
    # Build PostgreSQL array format
    my $pg_array = '{' . join(',', @string_elements) . '}';
    return $pg_array;
}

=head2 _convert_hash_to_composite

Convert a Perl hash reference to PostgreSQL composite type format

=cut

sub _convert_hash_to_composite {
    my ($self, $hash_ref) = @_;
    
    return '()' unless defined $hash_ref && ref($hash_ref) eq 'HASH';
    
    # Get keys in sorted order for consistency
    my @keys = sort keys %$hash_ref;
    
    # Handle empty hash
    return '()' unless @keys;
    
    # Convert values, preserving NULLs as empty fields
    my @fields = map {
        my $val = $hash_ref->{$_};
        if (!defined $val) {
            ''  # Empty field for NULL in composite type
        } elsif (ref($val)) {
            # Nested complex type
            $self->_convert_oracle_ref_to_pg_format($val, '')
        } else {
            # Quote strings that need quoting
            if ($val =~ /[,\s()]/ || $val eq '') {
                qq("$val")
            } else {
                $val
            }
        }
    } @keys;
    
    return '(' . join(',', @fields) . ')';
}

=head2 _normalize_timestamp

Normalize timestamp values by removing trailing zeros from fractional seconds
This ensures that timestamps like '2025-10-23 05:47:08.060520' and '2025-10-23 05:47:08.06052'
produce the same checksum.

=cut

sub _normalize_timestamp {
    my ($self, $value) = @_;
    
    return $value unless defined $value;
    
    # Match timestamp format with optional fractional seconds
    # Examples: 2025-10-23 05:47:08.060520, 2025-10-23 05:47:08.06052, 2025-10-23 05:47:08
    if ($value =~ /^(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\.?(\d*)$/) {
        my $base = $1;
        my $fractional = $2;
        
        if ($fractional) {
            # Remove trailing zeros from fractional seconds
            $fractional =~ s/0+$//;
            # If all zeros were removed, return just the base timestamp
            return $fractional ? "$base.$fractional" : $base;
        }
        
        return $base;
    }
    
    return $value;
}

=head2 generate_row_checksum

Generate a checksum for a data row containing complex types

=cut

sub generate_row_checksum {
    my ($self, $row_data, $data_types, $src_types, $table_name, $row_key) = @_;
    
    my @serialized_values = ();
    
    for (my $i = 0; $i < @$row_data; $i++) {
        my $value = $row_data->[$i];
        my $src_type = $src_types->[$i] || '';
        
        # Normalize timestamps to remove trailing zeros from fractional seconds
        $value = $self->_normalize_timestamp($value) if defined $value;
        
        if ($self->is_complex_type($src_type)) {
            # Track complex types found
            $self->{complex_types_found}{$table_name}{$src_type}++;
            
            # Convert Oracle Perl references to PostgreSQL format FIRST
            # This ensures Oracle's ARRAY/HASH refs are normalized to PostgreSQL strings
            my $normalized_value = $self->_convert_oracle_ref_to_pg_format($value, $src_type);
            
            # Serialize complex type with debug info
            push @serialized_values, $self->serialize_complex_value($normalized_value, $src_type, $table_name, $row_key, $i);
        } else {
            # For simple types, use the value as-is (or NULL)
            push @serialized_values, defined($value) ? $value : 'NULL';
        }
    }
    
    # Create a canonical string representation
    my $canonical_row = join('|', @serialized_values);
    
    # Generate hash
    return sha256_hex($canonical_row);
}

=head2 should_sample_row

Determine if a row should be sampled for validation

=cut

sub should_sample_row {
    my ($self, $row_number, $total_rows) = @_;
    
    return 0 unless $self->{enabled};
    
    # If we have total rows, use percentage sampling
    if ($total_rows && $total_rows > 0) {
        my $sample_rate = $self->{sample_percentage} / 100.0;
        return (rand() < $sample_rate);
    }
    
    # Otherwise use fixed sample size with uniform distribution
    my $sample_interval = int($total_rows / $self->{sample_size}) || 1;
    return ($row_number % $sample_interval == 0);
}

=head2 store_checksum

Store a checksum for later validation

=cut

sub store_checksum {
    my ($self, $table_name, $row_id, $checksum, $phase) = @_;
    
    $phase ||= 'export';
    
    $self->{checksums}{$table_name}{$row_id}{$phase} = $checksum;
}

=head2 validate_checksums

Compare checksums between export and import phases

=cut

sub validate_checksums {
    my ($self, $table_name, $pg_dbh, $schema_name) = @_;
    
    my $errors = [];
    my $total_checked = 0;
    my $mismatches = 0;
    
    if (!exists $self->{checksums}{$table_name}) {
        push @$errors, "No export checksums found for table $table_name";
        return { errors => $errors, total_checked => 0, mismatches => 0 };
    }
    
    # If PostgreSQL connection is provided, generate live import checksums
    if ($pg_dbh && $schema_name) {
        $self->_generate_live_import_checksums($table_name, $pg_dbh, $schema_name);
    }
    
    my $table_checksums = $self->{checksums}{$table_name};
    
    foreach my $row_id (keys %$table_checksums) {
        $total_checked++;
        
        my $export_checksum = $table_checksums->{$row_id}{export};
        my $import_checksum = $table_checksums->{$row_id}{import};
        
        if (!defined $export_checksum) {
            push @$errors, "Missing export checksum for $table_name row $row_id";
            next;
        }
        
        if (!defined $import_checksum) {
            push @$errors, "Missing import checksum for $table_name row $row_id";
            next;
        }
        
        if ($export_checksum ne $import_checksum) {
            $mismatches++;
            push @$errors, "Checksum mismatch for $table_name row $row_id: export=$export_checksum, import=$import_checksum";
        }
    }
    
    return {
        errors => $errors,
        total_checked => $total_checked,
        mismatches => $mismatches
    };
}

=head2 _generate_live_import_checksums

Generate import checksums from live PostgreSQL data during validation

=cut

sub _generate_live_import_checksums {
    my ($self, $table_name, $pg_dbh, $schema_name) = @_;
    
    print STDERR "DEBUG: _generate_live_import_checksums called for table $table_name in schema $schema_name\n";
    
    # Convert table name to lowercase for PostgreSQL compatibility
    my $pg_table_name = lc($table_name);
    
    # Get column information for the table
    my $col_sql = "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position";
    my $col_sth = $pg_dbh->prepare($col_sql);
    print STDERR "DEBUG: Executing column query with schema='$schema_name', table='$pg_table_name'\n";
    $col_sth->execute($schema_name, $pg_table_name);
    
    my @column_names = ();
    my @data_types = ();
    my @src_types = ();  # For complex type detection
    
    while (my ($col_name, $data_type) = $col_sth->fetchrow_array()) {
        push @column_names, $col_name;
        push @data_types, $data_type;
        
        # Map PostgreSQL types back to source types for complex type detection
        if ($data_type =~ /\[\]$/) {
            # Array types: integer[] -> INTEGER_ARRAY
            my $base_type = $data_type;
            $base_type =~ s/\[\]$//;
            push @src_types, uc("${base_type}_ARRAY");
        } elsif ($data_type eq 'ARRAY') {
            # PostgreSQL generic ARRAY type - need to determine base type from udt_name
            # For now, map to a recognizable array pattern
            push @src_types, 'ARRAY_TYPE';
        } elsif ($data_type eq 'USER-DEFINED') {
            # User-defined types: need to get actual type name
            push @src_types, 'OBJECT_TYPE';
        } elsif ($data_type =~ /^.*\..*_type$/i) {
            # Schema-qualified composite types: lolu.address_type -> ADDRESS_TYPE
            my ($schema, $type_name) = split(/\./, $data_type);
            push @src_types, uc($type_name);
        } elsif ($data_type =~ /_type$/i) {
            # Composite types: address_type -> ADDRESS_TYPE  
            push @src_types, uc($data_type);
        } else {
            push @src_types, uc($data_type);
        }
    }
    $col_sth->finish();
    
    print STDERR "DEBUG: Found columns: " . join(', ', @column_names) . "\n";
    print STDERR "DEBUG: Data types: " . join(', ', @data_types) . "\n"; 
    print STDERR "DEBUG: Source types: " . join(', ', @src_types) . "\n";
    
    if (!@column_names) {
        print STDERR "DEBUG: No columns found for table $table_name\n";
        return;  # No columns found
    }
    
    # Query the primary key columns for this table
    my @pk_column_names = ();
    my @pk_indices = ();
    my $pk_sql = "SELECT a.attname 
                  FROM pg_index i
                  JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                  WHERE i.indrelid = (
                      SELECT oid FROM pg_class 
                      WHERE relname = ? AND relnamespace = (
                          SELECT oid FROM pg_namespace WHERE nspname = ?
                      )
                  )
                  AND i.indisprimary
                  ORDER BY array_position(i.indkey, a.attnum)";
    
    my $pk_sth = $pg_dbh->prepare($pk_sql);
    if ($pk_sth && $pk_sth->execute($pg_table_name, $schema_name)) {
        while (my ($pk_col) = $pk_sth->fetchrow_array()) {
            push @pk_column_names, $pk_col;
            # Find index of this PK column in the column list
            for (my $i = 0; $i < @column_names; $i++) {
                if (lc($column_names[$i]) eq lc($pk_col)) {
                    push @pk_indices, $i;
                    last;
                }
            }
        }
        $pk_sth->finish();
        
        if (@pk_column_names) {
            print STDERR "DEBUG: Found primary key columns: " . join(', ', @pk_column_names) . " at indices: " . join(', ', @pk_indices) . "\n";
        } else {
            print STDERR "DEBUG: No primary key found for table $table_name, will use first column\n";
        }
    }
    
    # Query the actual data
    my $schema_prefix = $schema_name eq 'public' ? '' : "$schema_name.";
    my $data_sql = "SELECT " . join(', ', @column_names) . " FROM ${schema_prefix}$pg_table_name ORDER BY " . $column_names[0];
    my $data_sth = $pg_dbh->prepare($data_sql);
    
    if (!$data_sth || !$data_sth->execute()) {
        print STDERR "DEBUG: Data query failed for table $pg_table_name in schema $schema_name\n";
        return;  # Query failed
    }
    
    my $row_number = 0;
    while (my @row_data = $data_sth->fetchrow_array()) {
        $row_number++;
        
        # Build row identifier from primary key values
        my $row_id_value;
        if (@pk_indices) {
            # Use primary key column values (handles composite keys)
            $row_id_value = join('_', map { defined $row_data[$_] ? $row_data[$_] : 'NULL' } @pk_indices);
        } elsif (defined $row_data[0]) {
            # Fall back to first column if no PK found
            $row_id_value = $row_data[0];
        } else {
            # Last resort: use row number
            $row_id_value = $row_number;
        }
        my $row_id = "${table_name}_${row_id_value}";
        
        # Convert PostgreSQL array references to string format for processing
        my @processed_row_data = ();
        for my $i (0..$#row_data) {
            my $value = $row_data[$i];
            if (ref($value) eq 'ARRAY') {
                # Convert PostgreSQL array reference to string format: {elem1,elem2,elem3}
                my $array_str = '{' . join(',', @$value) . '}';
                push @processed_row_data, $array_str;
            } else {
                push @processed_row_data, $value;
            }
        }
        
        print STDERR "DEBUG: Processing row $row_id with data: " . join('|', map { defined $_ ? $_ : 'NULL' } @processed_row_data) . "\n";
        
        # Generate import checksum with debug data collection
        my $import_checksum = $self->generate_row_checksum(
            \@processed_row_data,
            \@data_types,
            \@src_types,
            $table_name,
            $row_id
        );
        
        print STDERR "DEBUG: Generated checksum $import_checksum for row $row_id\n";
        
        # Store the import checksum
        $self->store_checksum($table_name, $row_id, $import_checksum, 'import');
    }
    
    $data_sth->finish();
    
    print STDERR "DEBUG: Debug data after processing: " . scalar(keys %{$self->{row_normalization_debug}}) . " tables\n";
}

=head2 save_checksums

Save checksums to a file

=cut

sub save_checksums {
    my ($self, $filename) = @_;
    
    $filename ||= $self->{checksum_file};
    
    open my $fh, '>', $filename or die "Cannot open checksum file $filename: $!";
    print $fh JSON::PP->new->pretty->encode($self->{checksums});
    close $fh;
}

=head2 load_checksums

Load checksums from a file

=cut

sub load_checksums {
    my ($self, $filename) = @_;
    
    $filename ||= $self->{checksum_file};
    
    return unless -f $filename;
    
    open my $fh, '<', $filename or die "Cannot open checksum file $filename: $!";
    local $/;
    my $content = <$fh>;
    close $fh;
    
    my $data = JSON::PP->new->decode($content);
    $self->{checksums} = $data if ref $data eq 'HASH';
}

=head2 generate_validation_report

Generate a comprehensive validation report

=cut

sub generate_validation_report {
    my ($self, $filename) = @_;
    
    $filename ||= $self->{validation_output};
    
    open my $fh, '>', $filename or die "Cannot open validation report file $filename: $!";
    
    print $fh "Ora2Pg Complex Type Data Integrity Validation Report\n";
    print $fh "=" x 60 . "\n\n";
    print $fh "Generated: " . localtime() . "\n\n";
    
    # Summary statistics section removed as requested
    
    # Complex types found
    if (keys %{$self->{complex_types_found}}) {
        print $fh "COMPLEX TYPES FOUND\n";
        print $fh "-" x 30 . "\n";
        foreach my $table (sort keys %{$self->{complex_types_found}}) {
            print $fh "Table: $table\n";
            foreach my $type (sort keys %{$self->{complex_types_found}{$table}}) {
                my $count = $self->{complex_types_found}{$table}{$type};
                print $fh "  $type: $count occurrences\n";
            }
            print $fh "\n";
        }
    }
    
    # Validation results by table
    print $fh "VALIDATION RESULTS\n";
    print $fh "-" x 30 . "\n";
    
    my $total_errors = 0;
    my $total_mismatches = 0;
    my $total_checked = 0;
    
    foreach my $table (sort keys %{$self->{checksums}}) {
        my $result = $self->validate_checksums($table);
        
        $total_checked += $result->{total_checked};
        $total_mismatches += $result->{mismatches};
        $total_errors += scalar @{$result->{errors}};
        
        print $fh "Table: $table\n";
        print $fh "  Rows checked: $result->{total_checked}\n";
        print $fh "  Mismatches: $result->{mismatches}\n";
        print $fh "  Errors: " . scalar(@{$result->{errors}}) . "\n";
        
        if (@{$result->{errors}}) {
            print $fh "  Error details:\n";
            foreach my $error (@{$result->{errors}}) {
                print $fh "    - $error\n";
            }
        }
        
        # Show semantic normalization details for ALL tables (not just those with errors)
        if ($self->{row_normalization_debug}{$table}) {
            print $fh "  Semantic normalization details:\n";
            foreach my $row_key (sort keys %{$self->{row_normalization_debug}{$table}}) {
                my $debug_info = $self->{row_normalization_debug}{$table}{$row_key};
                if (@$debug_info) {
                    print $fh "    Row $row_key complex type processing:\n";
                    foreach my $col_debug (@$debug_info) {
                        print $fh "      Column $col_debug->{column_index} ($col_debug->{src_type}):\n";
                        print $fh "        Original:   '$col_debug->{original_value}'\n";
                        print $fh "        Normalized: '$col_debug->{normalized_value}'\n";
                        print $fh "        Serialized: '$col_debug->{serialized_value}'\n";
                    }
                }
            }
        } else {
            print $fh "  Semantic normalization details: No debugging data captured\n";
        }
        
        # Show checksum comparison details for ALL rows
        if (exists $self->{checksums}{$table}) {
            print $fh "  Checksum details:\n";
            foreach my $row_id (sort keys %{$self->{checksums}{$table}}) {
                my $export_checksum = $self->{checksums}{$table}{$row_id}{export} || 'N/A';
                my $import_checksum = $self->{checksums}{$table}{$row_id}{import} || 'N/A';
                my $match_status = ($export_checksum eq $import_checksum && $export_checksum ne 'N/A') ? '✓ MATCH' : '✗ MISMATCH';
                print $fh "    Row $row_id: export=$export_checksum, import=$import_checksum ($match_status)\n";
            }
        }
        print $fh "\n";
    }
    
    # Overall summary
    print $fh "OVERALL VALIDATION SUMMARY\n";
    print $fh "-" x 30 . "\n";
    print $fh "Total rows checked: $total_checked\n";
    print $fh "Total mismatches: $total_mismatches\n";
    print $fh "Total errors: $total_errors\n";
    
    if ($total_checked > 0) {
        my $success_rate = sprintf("%.2f", (($total_checked - $total_mismatches) / $total_checked) * 100);
        print $fh "Success rate: $success_rate%\n";
    }
    
    if ($total_mismatches == 0 && $total_errors == 0) {
        print $fh "\n✓ ALL VALIDATIONS PASSED - Data integrity verified!\n";
    } else {
        print $fh "\n✗ VALIDATION FAILURES DETECTED - Please review the errors above.\n";
    }
    
    close $fh;
    
    return {
        total_checked => $total_checked,
        total_mismatches => $total_mismatches,
        total_errors => $total_errors
    };
}

1;

__END__

=head1 AUTHOR

Gilles Darold E<lt>gilles@darold.netE<gt>

=head1 COPYRIGHT

Copyright (c) 2000-2025 Gilles Darold - All rights reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
any later version.

=cut
