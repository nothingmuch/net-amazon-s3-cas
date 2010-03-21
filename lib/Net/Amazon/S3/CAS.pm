package Net::Amazon::S3::CAS;
use Moose;

use HTTP::Date;
use Carp;
use File::Basename;
use MooseX::Types::URI qw(Uri);

use namespace::clean -except => 'meta';

has fork_manager => (
    isa => "Object",
    is  => "ro",
);

has bucket => (
    isa => "Net::Amazon::S3::Bucket",
    is  => "ro",
    required => 1,
);

has collection => (
    does => "Net::Amazon::S3::CAS::Collection",
    is   => "ro",
    required => 1,
);

has prefix => (
    isa      => "Str",
    is       => "ro",
    required => 1,
    default  => "",
);

has extra_headers => (
    isa      => "HashRef",
    is       => "ro",
    required => 1,
    default  => sub { +{} },
);

has max_age => (
    isa => "Maybe[Int]",
    is  => "ro",
    default => 10 * 365 * 24 * 60 * 60, # 10 years
);

has guess_mimetype => (
    isa     => "Bool",
    is      => "ro",
    default => 1,
);

has prune => (
    isa => "Bool",
    is  => "ro",
);

has public => (
    isa => "Bool",
    is  => "ro",
    default => 1,
);

has include_name => (
    isa => "Bool",
    is  => "ro",
    default => 0,
);

has only_basename => (
    isa => "Bool",
    is  => "ro",
    default => 1,
);

has delimiter => (
    isa => "Str",
    is  => "ro",
    default => ".",
);

has base_uri => (
    isa => Uri,
    is  => "ro",
    coerce => 1,
    lazy_build => 1,
);

sub _build_base_uri {
    my $self = shift;
    return URI::->new( "http://s3.amazonaws.com/" . $self->bucket->bucket . "/" );
}


has mime_types_directory => (
    isa        => "Object",
    handles    => [qw(mimeTypeOf)],
    lazy_build => 1,
);


sub _build_mime_types_directory {
    require MIME::Types;
    MIME::Types->new;
}

sub sync {
    my $self = shift;

    my $stream = $self->collection->entries;

    my %keys;

    my $pm = $self->fork_manager;

    my %uris;

    while ( my $block = $stream->next ) {

        my %entries;

        foreach my $entry ( @$block ) {
            my $key = $self->entry_key($entry);

            unless ( $keys{$key}++ ) {
                $entries{$key} = $entry;
            }
        }

        foreach my $key ( keys %entries ) {
            my $entry = $entries{$key};

            if ( my $name = $entry->name ) {
                $uris{$name} = $self->entry_uri($key, $entry);
            }

            $pm->start and next if $pm;

            # nasty hack
            no warnings 'redefine';
            local *Git::DESTROY = sub { } if $pm;

            $self->sync_entry($key, $entry);

            $pm->finish if $pm;
        }
    }

    if ( $self->prune ) {
        $self->prune_keys(\%keys);
    }

    $pm->wait_all_children if $pm;

    return \%uris;
}

sub prune_keys {
    my ( $self, $keys ) = @_;

    my @keys = map { $_->{key} } @{ $self->bucket->list_all({ prefix => $self->prefix })->{keys} };

    my @prune = grep { not exists $keys->{$_} } @keys;

    my $pm = $self->fork_manager;

    foreach my $key ( @prune ) {
        $pm->start and next if $pm;

        $self->bucket->delete_key($key);

        $pm->finish if $pm;
    }
}

sub sync_entry {
    my ( $self, $key, $entry ) = @_;

    unless ( $self->verify_entry($key, $entry) ) {
        $self->upload_entry($key, $entry);
    }
}

sub entry_uri {
    my ( $self, $key, $entry ) = @_;

    my $c = $self->base_uri->clone;

    $c->path( $c->path . $key );

    return $c;
}

sub mangle_key {
    my ( $self, $key ) = @_;

    $self->prefix . $key;
}

sub entry_key {
    my ( $self, $entry ) = @_;

    my $key = $entry->key;

    if ( $self->include_name && defined(my $name = $entry->name) ) {
        local $File::Basename::Fileparse_igncase = 1;
        my ( $basename, $path, $ext ) = fileparse($name, qr/\.(?:[^\.\s]+)/);

        return $self->mangle_key( join $self->delimiter, ( $self->only_basename ? $basename : $name ), $key . $ext );
    } else {
        return $self->mangle_key( $key );
    }
}


sub verify_entry {
    my ( $self, $key, $entry ) = @_;

    if ( my $head = $self->bucket->head_key($key) ) {
        if ( $head->{"content-length"} == $entry->size ) {
            return 1;
        }
    }
}

sub upload_entry {
    my ( $self, $key, $entry ) = @_;

    $self->bucket->delete_key($key);

    if ( $entry->prefer_handle && $entry->can("filename") ) {
        $self->upload_entry_file($key, $entry);
    } else {
        $self->upload_entry_string($key, $entry);
    }

    unless ( $self->verify_entry($key, $entry) ) {
        croak "Uploading of " . $entry->key . " failed";
    }
}

sub upload_entry_file {
    my ( $self, $key, $entry ) = @_;

    $self->bucket->add_key_filename( $key, $entry->blob->filename, $self->entry_headers($entry) );
}

sub upload_entry_string {
    my ( $self, $key, $entry ) = @_;

    $self->bucket->add_key( $key, scalar($entry->slurp), $self->entry_headers($entry) );
}

sub entry_headers {
    my ( $self, $entry ) = @_;

    return {
        ( $self->public ? ( acl_short => "public-read" ) : () ),
        $self->entry_headers_etag($entry),
        $self->entry_headers_cache($entry),
        $self->entry_headers_type($entry),
        $self->entry_headers_extra($entry),
        %{ $entry->headers },
    };
}

sub entry_headers_etag {
    my ( $self, $entry ) = @_;

    return ( ETag => $entry->key ), # Amazon seems to ignore this and use MD5, but who cares
}

sub entry_headers_cache {
    my ( $self, $entry ) = @_;

    if ( my $age = $self->max_age ) {
        return (
            "Expires"       => time2str( $age + time() ),
            "Cache-Control" => "public; max-age=$age"
        );
    } else {
        return ();
    }
}

sub entry_headers_type {
    my ( $self, $entry ) = @_;

    if ( $self->guess_mimetype and my $name = $entry->name ) {
        if ( my $type = $self->mimeTypeOf($name) ) {
            return ( 'Content-Type' => $type );
        }
    }

    return ();
}



sub entry_headers_extra {
    my ( $self, $entry ) = @_;

    %{ $self->extra_headers }
}

__PACKAGE__->meta->make_immutable;

__PACKAGE__

__END__
