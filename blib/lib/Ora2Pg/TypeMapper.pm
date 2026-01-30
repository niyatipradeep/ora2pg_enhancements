package Ora2Pg::TypeMapper;

#------------------------------------------------------------------------------
# Project  : Oracle to PostgreSQL database schema converter
# Name     : TypeMapper.pm
# Language : Perl
# Authors  : Auto Type Mapping Implementation
# Copyright: Copyright (c) 2000-2025 : Gilles DAROLD
# Function : Automatic detection and mapping of Oracle custom types
#------------------------------------------------------------------------------

use strict;
use warnings;

=head1 NAME

Ora2Pg::TypeMapper - Automatic Oracle custom type to PostgreSQL type mapping

=head1 SYNOPSIS

    use Ora2Pg::TypeMapper;
    
    my $mapper = Ora2Pg::TypeMapper->new(
        dbh => $oracle_dbh,
        schema => 'MY_SCHEMA'
    );
    
    # Analyze and map all custom types
    my $mappings = $mapper->analyze_and_map_types();
    
    # Generate verification report
    $mapper->generate_mapping_report('output/type_mapping_report.txt');

=head1 DESCRIPTION

This module automatically detects Oracle custom types (OBJECT, VARRAY, NESTED TABLE)
and generates appropriate PostgreSQL type mappings without requiring manual MODIFY_TYPE
configuration.

=cut

=head1 METHODS

=head2 new

Constructor. Creates a new TypeMapper instance.

=cut

sub new {
    my ($class, %args) = @_;
    
    my $self = {
        dbh => $args{dbh},
        schema => uc($args{schema} || ''),  # Convert schema to uppercase for Oracle
        prefix => $args{prefix} || 'ALL',
        sysusers => $args{sysusers} || [],
        mappings => {},
        type_info => {},
        report => [],
    };
    
    return bless $self, $class;
}

=head2 analyze_and_map_types

Analyzes all custom types in the Oracle schema and generates automatic mappings.

Returns a hash reference with table->column->type mappings.

=cut

sub analyze_and_map_types {
    my ($self) = @_;
    
    # Step 1: Fetch all custom types from Oracle
    my $custom_types = $self->_fetch_custom_types();
    
    # Step 2: Analyze each type and determine mapping
    foreach my $type_name (keys %$custom_types) {
        my $type_info = $custom_types->{$type_name};
        my $mapping = $self->_determine_mapping($type_name, $type_info);
        
        $self->{type_info}{$type_name} = {
            oracle_type => $type_info->{typecode},
            structure => $type_info->{structure},
            pg_mapping => $mapping->{pg_type},
            strategy => $mapping->{strategy},
            confidence => $mapping->{confidence},
            support_level => $mapping->{support_level},
            warning_message => $mapping->{warning_message},
            recommendation => $mapping->{recommendation},
        };
    }
    
    # Step 3: Find which tables/columns use these types
    $self->_map_types_to_columns();
    
    return $self->{mappings};
}

=head2 _fetch_custom_types

Internal method to fetch all user-defined types from Oracle.

=cut

sub _fetch_custom_types {
    my ($self) = @_;
    
    my %types;
    
    # Query to get all user-defined types
    my $sql = qq{
        SELECT 
            t.TYPE_NAME,
            t.TYPECODE,
            t.ATTRIBUTES,
            t.OWNER
        FROM $self->{prefix}_TYPES t
        WHERE t.OWNER = ?
          AND t.TYPECODE IN ('OBJECT', 'COLLECTION', 'VARRAY', 'TABLE')
          AND t.TYPE_NAME NOT LIKE 'SYS_%'
        ORDER BY t.TYPE_NAME
    };
    
    my $sth = $self->{dbh}->prepare($sql);
    $sth->execute($self->{schema});
    
    while (my $row = $sth->fetchrow_hashref()) {
        my $type_name = $row->{TYPE_NAME};
        
        $types{$type_name} = {
            typecode => $row->{TYPECODE},
            attributes => $row->{ATTRIBUTES},
            owner => $row->{OWNER},
        };
        
        # Get detailed structure
        $types{$type_name}{structure} = $self->_analyze_type_structure($type_name);
    }
    
    $sth->finish();
    
    return \%types;
}

=head2 _analyze_type_structure

Analyzes the internal structure of a custom type.

=cut

sub _analyze_type_structure {
    my ($self, $type_name) = @_;
    
    my $structure = {
        attributes => [],
        element_type => undef,
        is_simple_array => 0,
        is_object => 0,
    };
    
    # For OBJECT types, get attributes
    my $sql = qq{
        SELECT 
            ATTR_NAME,
            ATTR_TYPE_NAME,
            LENGTH,
            PRECISION,
            SCALE,
            ATTR_NO
        FROM $self->{prefix}_TYPE_ATTRS
        WHERE OWNER = ? AND TYPE_NAME = ?
        ORDER BY ATTR_NO
    };
    
    my $sth = $self->{dbh}->prepare($sql);
    $sth->execute($self->{schema}, $type_name);
    
    while (my $row = $sth->fetchrow_hashref()) {
        push @{$structure->{attributes}}, {
            name => $row->{ATTR_NAME},
            type => $row->{ATTR_TYPE_NAME},
            length => $row->{LENGTH},
            precision => $row->{PRECISION},
            scale => $row->{SCALE},
        };
    }
    
    $sth->finish();
    
    # Check if it's a simple array (VARRAY/TABLE of primitive type)
    if (@{$structure->{attributes}} == 0) {
        # This is a collection type, get element type
        my $coll_sql = qq{
            SELECT ELEM_TYPE_NAME
            FROM $self->{prefix}_COLL_TYPES
            WHERE OWNER = ? AND TYPE_NAME = ?
        };
        
        my $coll_sth = $self->{dbh}->prepare($coll_sql);
        $coll_sth->execute($self->{schema}, $type_name);
        
        if (my $coll_row = $coll_sth->fetchrow_hashref()) {
            $structure->{element_type} = $coll_row->{ELEM_TYPE_NAME};
            $structure->{is_simple_array} = $self->_is_primitive_type($structure->{element_type});
        }
        
        $coll_sth->finish();
    } else {
        $structure->{is_object} = 1;
    }
    
    return $structure;
}

=head2 _determine_mapping

Determines the appropriate PostgreSQL mapping for an Oracle custom type.

=cut

sub _determine_mapping {
    my ($self, $type_name, $type_info) = @_;
    
    my $mapping = {
        pg_type => undef,
        strategy => 'unknown',
        confidence => 0,
        support_level => 'unknown',
        warning_message => '',
        recommendation => '',
    };
    
    my $structure = $type_info->{structure};
    my $typecode = $type_info->{typecode};
    
    # RULE 1: Simple array types (VARRAY/TABLE of primitive type) -> PostgreSQL array
    if (($typecode eq 'VARRAY' || $typecode eq 'TABLE' || $typecode eq 'COLLECTION') 
        && $structure->{is_simple_array}) {
        
        my $pg_base_type = $self->_map_oracle_to_pg_type($structure->{element_type});
        $mapping->{pg_type} = $pg_base_type . '[]';
        $mapping->{strategy} = 'simple_array';
        $mapping->{confidence} = 95;
        $mapping->{support_level} = 'FULLY_SUPPORTED';
        $mapping->{warning_message} = '';  # No warning needed
        
        push @{$self->{report}}, {
            type => $type_name,
            oracle => "$typecode OF $structure->{element_type}",
            postgres => $mapping->{pg_type},
            strategy => 'Automatic: Simple array mapping',
        };
    }
    # RULE 2: OBJECT types -> Check for special cases
    elsif ($typecode eq 'OBJECT' && $structure->{is_object}) {
        
        # Check for nested objects, LOBs, and other problematic attributes
        my $analysis = $self->_analyze_object_attributes($structure->{attributes});
        
        $mapping->{pg_type} = lc($type_name);
        $mapping->{strategy} = 'composite_type';
        
        if ($analysis->{has_lobs}) {
            $mapping->{confidence} = 40;
            $mapping->{support_level} = 'PARTIALLY_SUPPORTED';
            $mapping->{warning_message} = "OBJECT type '$type_name' contains LOB attributes (CLOB/BLOB). " .
                                          "Large object handling in composite types may have issues with data export.";
            $mapping->{recommendation} = "Test data export thoroughly. Consider extracting LOB columns separately " .
                                        "or using PostgreSQL TEXT/BYTEA types directly instead of composite.";
        }
        elsif ($analysis->{has_nested_objects}) {
            $mapping->{confidence} = 70;
            $mapping->{support_level} = 'PARTIALLY_SUPPORTED';
            $mapping->{warning_message} = "OBJECT type '$type_name' contains nested OBJECT attributes: " .
                                          join(', ', @{$analysis->{nested_object_attrs}}) . ". " .
                                          "Data export format may require manual verification.";
            $mapping->{recommendation} = "Verify nested composite type data format after export. " .
                                        "PostgreSQL format: ('outer_val', ROW('inner_val1', 'inner_val2'))";
        }
        elsif ($analysis->{has_spatial}) {
            $mapping->{confidence} = 0;
            $mapping->{support_level} = 'UNSUPPORTED';
            $mapping->{warning_message} = "OBJECT type '$type_name' contains SDO_GEOMETRY or spatial attributes. " .
                                          "Spatial types are NOT automatically supported.";
            $mapping->{recommendation} = "MANUAL MIGRATION REQUIRED: Use PostGIS extension and manually map to geometry types. " .
                                        "Example: MODIFY_TYPE tablename:columnname:geometry(POINT,4326)";
        }
        elsif ($analysis->{has_xmltype}) {
            $mapping->{confidence} = 20;
            $mapping->{support_level} = 'UNSUPPORTED';
            $mapping->{warning_message} = "OBJECT type '$type_name' contains XMLType attributes. " .
                                          "XMLType has limited support and may not preserve XML functionality.";
            $mapping->{recommendation} = "Consider using PostgreSQL XML type or TEXT. " .
                                        "Add MODIFY_TYPE directive: tablename:columnname:xml or text";
        }
        elsif ($analysis->{has_ref_types}) {
            $mapping->{confidence} = 0;
            $mapping->{support_level} = 'UNSUPPORTED';
            $mapping->{warning_message} = "OBJECT type '$type_name' contains REF attributes. " .
                                          "REF types (object references) do NOT exist in PostgreSQL.";
            $mapping->{recommendation} = "MANUAL MIGRATION REQUIRED: Replace REF with foreign key relationships. " .
                                        "Restructure schema to use INTEGER foreign keys instead.";
        }
        else {
            # Simple flat object
            $mapping->{confidence} = 90;
            $mapping->{support_level} = 'FULLY_SUPPORTED';
            $mapping->{warning_message} = '';
        }
        
        push @{$self->{report}}, {
            type => $type_name,
            oracle => "OBJECT with " . scalar(@{$structure->{attributes}}) . " attributes",
            postgres => $mapping->{pg_type} . ' (composite type)',
            strategy => 'Automatic: Composite type mapping',
            support_level => $mapping->{support_level},
            confidence => $mapping->{confidence},
        };
    }
    # RULE 3: Array of non-primitive types (collection of objects)
    elsif (($typecode eq 'VARRAY' || $typecode eq 'TABLE' || $typecode eq 'COLLECTION')
           && !$structure->{is_simple_array}) {
        
        my $element_type = $structure->{element_type} || 'UNKNOWN';
        $mapping->{pg_type} = lc($element_type) . '[]';
        $mapping->{strategy} = 'array_of_objects';
        $mapping->{confidence} = 65;
        $mapping->{support_level} = 'PARTIALLY_SUPPORTED';
        $mapping->{warning_message} = "$typecode '$type_name' is an array of OBJECT type '$element_type'. " .
                                      "Schema mapping works, but data export format is complex.";
        $mapping->{recommendation} = "Verify data format: {(val1,val2),(val3,val4)}. " .
                                    "May need manual formatting adjustment for PostgreSQL import.";
        
        push @{$self->{report}}, {
            type => $type_name,
            oracle => "$typecode OF $element_type (object)",
            postgres => $mapping->{pg_type},
            strategy => 'Array of objects',
            support_level => $mapping->{support_level},
            confidence => $mapping->{confidence},
        };
    }
    # RULE 4: Complex nested types -> Keep as composite (will need fix_syntax)
    else {
        $mapping->{pg_type} = lc($type_name);
        $mapping->{strategy} = 'composite_type';
        $mapping->{confidence} = 70;
        $mapping->{support_level} = 'PARTIALLY_SUPPORTED';
        $mapping->{warning_message} = "$typecode '$type_name' is a complex nested type. " .
                                      "Automatic mapping applied but may need manual verification.";
        $mapping->{recommendation} = "Test data export and PostgreSQL import thoroughly.";
        
        push @{$self->{report}}, {
            type => $type_name,
            oracle => "$typecode (complex)",
            postgres => $mapping->{pg_type} . ' (composite type)',
            strategy => 'Default: Composite type mapping',
            support_level => $mapping->{support_level},
            confidence => $mapping->{confidence},
        };
    }
    
    return $mapping;
}

=head2 _analyze_object_attributes

Analyzes OBJECT type attributes to detect problematic patterns.

=cut

sub _analyze_object_attributes {
    my ($self, $attributes) = @_;
    
    my $analysis = {
        has_lobs => 0,
        has_nested_objects => 0,
        has_spatial => 0,
        has_xmltype => 0,
        has_ref_types => 0,
        nested_object_attrs => [],
        lob_attrs => [],
    };
    
    foreach my $attr (@$attributes) {
        my $attr_type = uc($attr->{type});
        
        # Check for LOB types
        if ($attr_type =~ /^(CLOB|BLOB|NCLOB|BFILE)$/) {
            $analysis->{has_lobs} = 1;
            push @{$analysis->{lob_attrs}}, $attr->{name};
        }
        # Check for spatial types
        elsif ($attr_type =~ /^(SDO_GEOMETRY|ST_GEOMETRY|MDSYS\.SDO_GEOMETRY)$/) {
            $analysis->{has_spatial} = 1;
        }
        # Check for XML types
        elsif ($attr_type =~ /^(XMLTYPE|SYS\.XMLTYPE)$/) {
            $analysis->{has_xmltype} = 1;
        }
        # Check for REF types
        elsif ($attr_type =~ /^REF\s+/ || $attr_type =~ /_REF$/) {
            $analysis->{has_ref_types} = 1;
        }
        # Check for nested object types (not a primitive)
        elsif (!$self->_is_primitive_type($attr_type)) {
            $analysis->{has_nested_objects} = 1;
            push @{$analysis->{nested_object_attrs}}, $attr->{name} . '(' . $attr_type . ')';
        }
    }
    
    return $analysis;
}

=head2 _map_types_to_columns

Maps custom types to the tables and columns that use them.

=cut

sub _map_types_to_columns {
    my ($self) = @_;
    
    my $sql = qq{
        SELECT 
            TABLE_NAME,
            COLUMN_NAME,
            DATA_TYPE
        FROM $self->{prefix}_TAB_COLUMNS
        WHERE OWNER = ?
          AND DATA_TYPE IN (SELECT TYPE_NAME FROM $self->{prefix}_TYPES WHERE OWNER = ?)
        ORDER BY TABLE_NAME, COLUMN_ID
    };
    
    my $sth = $self->{dbh}->prepare($sql);
    $sth->execute($self->{schema}, $self->{schema});
    
    while (my $row = $sth->fetchrow_hashref()) {
        my $table = $row->{TABLE_NAME};
        my $column = $row->{COLUMN_NAME};
        my $type = $row->{DATA_TYPE};
        
        if (exists $self->{type_info}{$type}) {
            $self->{mappings}{$table}{$column} = $self->{type_info}{$type}{pg_mapping};
        }
    }
    
    $sth->finish();
}

=head2 _is_primitive_type

Checks if a type is a primitive Oracle type.

=cut

sub _is_primitive_type {
    my ($self, $type_name) = @_;
    
    my %primitive_types = (
        'VARCHAR2' => 1, 'VARCHAR' => 1, 'CHAR' => 1, 'NCHAR' => 1,
        'NUMBER' => 1, 'INTEGER' => 1, 'FLOAT' => 1, 'DECIMAL' => 1,
        'DATE' => 1, 'TIMESTAMP' => 1, 'CLOB' => 1, 'BLOB' => 1,
        'RAW' => 1, 'LONG' => 1,
    );
    
    return exists $primitive_types{uc($type_name)};
}

=head2 _map_oracle_to_pg_type

Maps Oracle primitive type to PostgreSQL type.

=cut

sub _map_oracle_to_pg_type {
    my ($self, $oracle_type) = @_;
    
    my %type_map = (
        'VARCHAR2' => 'text',
        'VARCHAR' => 'varchar',
        'CHAR' => 'char',
        'NCHAR' => 'char',
        'NVARCHAR2' => 'varchar',
        'NUMBER' => 'numeric',
        'INTEGER' => 'integer',
        'FLOAT' => 'double precision',
        'DECIMAL' => 'decimal',
        'DATE' => 'timestamp',
        'TIMESTAMP' => 'timestamp',
        'CLOB' => 'text',
        'BLOB' => 'bytea',
        'RAW' => 'bytea',
    );
    
    return $type_map{uc($oracle_type)} || 'text';
}

=head2 generate_mapping_report

Generates a human-readable report of all type mappings.

=cut

sub generate_mapping_report {
    my ($self, $output_file) = @_;
    
    open my $fh, '>', $output_file or die "Cannot create report file: $!";
    
    print $fh "=" x 80 . "\n";
    print $fh "Oracle Custom Type Automatic Mapping Report\n";
    print $fh "Generated: " . scalar(localtime()) . "\n";
    print $fh "Schema: $self->{schema}\n";
    print $fh "=" x 80 . "\n\n";
    
    print $fh "CUSTOM TYPE MAPPINGS:\n";
    print $fh "-" x 80 . "\n";
    printf $fh "%-25s %-30s %-20s\n", "Oracle Type", "Oracle Definition", "PostgreSQL Type";
    print $fh "-" x 80 . "\n";
    
    foreach my $item (@{$self->{report}}) {
        printf $fh "%-25s %-30s %-20s\n", 
            $item->{type}, 
            substr($item->{oracle}, 0, 30),
            substr($item->{postgres}, 0, 20);
    }
    
    print $fh "\n\nCOLUMN MAPPINGS APPLIED:\n";
    print $fh "-" x 80 . "\n";
    printf $fh "%-30s %-25s %-20s\n", "Table", "Column", "PostgreSQL Type";
    print $fh "-" x 80 . "\n";
    
    foreach my $table (sort keys %{$self->{mappings}}) {
        foreach my $column (sort keys %{$self->{mappings}{$table}}) {
            printf $fh "%-30s %-25s %-20s\n",
                $table,
                $column,
                $self->{mappings}{$table}{$column};
        }
    }
    
    print $fh "\n" . "=" x 80 . "\n";
    print $fh "Total custom types mapped: " . scalar(@{$self->{report}}) . "\n";
    print $fh "Total columns affected: " . $self->_count_columns() . "\n";
    print $fh "=" x 80 . "\n";
    
    close $fh;
    
    return $output_file;
}

sub _count_columns {
    my ($self) = @_;
    my $count = 0;
    foreach my $table (keys %{$self->{mappings}}) {
        $count += scalar keys %{$self->{mappings}{$table}};
    }
    return $count;
}

sub get_modify_type_config {
    my ($self) = @_;
    
    my @config_lines;
    
    foreach my $table (sort keys %{$self->{mappings}}) {
        foreach my $column (sort keys %{$self->{mappings}{$table}}) {
            my $pg_type = $self->{mappings}{$table}{$column};
            push @config_lines, "$table:$column:$pg_type";
        }
    }
    
    return join(',', @config_lines);
}

1;

__END__

=head1 AUTHOR

Ora2Pg Team

=head1 COPYRIGHT

Copyright (c) 2000-2025 Gilles DAROLD

=cut
