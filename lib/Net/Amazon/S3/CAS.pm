package Net::Amazon::S3::CAS;
use Moose;

use HTTP::Date;
use Carp;

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

has prune => (
    isa => "Bool",
    is  => "ro",
);

sub sync {
    my $self = shift;

    my $stream = $self->collection->entries;

    my %keys;

    my $filtered = $stream->filter(sub {[ grep { !$keys{$self->entry_key($_)}++ } @$_ ]});

    my $pm = $self->fork_manager;

    while ( my $block = $filtered->next ) {
        foreach my $entry ( @$block ) {
            $pm->start and next if $pm;

            $self->sync_entry($entry);

            $pm->finish if $pm;
        }
    }

    if ( $self->prune ) {
        $self->prune_keys(\%keys);
    }
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
    my ( $self, $entry ) = @_;

    unless ( $self->verify_entry($entry) ) {
        $self->upload_entry($entry);
    }
}

sub mangle_key {
    my ( $self, $key ) = @_;

    $self->prefix . $key;
}

sub entry_key {
    my ( $self, $entry ) = @_;

    $self->mangle_key( $entry->key );
}

sub verify_entry {
    my ( $self, $entry ) = @_;

    if ( my $head = $self->bucket->head_key( $self->entry_key($entry) ) ) {
        if ( $head->{"content-length"} == $entry->size ) {
            return 1;
        }
    }
}

sub upload_entry {
    my ( $self, $entry ) = @_;

    if ( $entry->prefer_handle && $entry->can("filename") ) {
        $self->upload_entry_file($entry);
    } else {
        $self->upload_entry_string($entry);
    }

    unless ( $self->verify_entry($entry) ) {
        croak "Uploading of " . $entry->key . " failed";
    }
}

sub upload_entry_file {
    my ( $self, $entry ) = @_;

    $self->bucket->delete_key($self->entry_key($entry));
    $self->bucket->add_key_filename( $self->entry_key($entry), $entry->blob->filename, $self->entry_headers($entry) );
}

sub upload_entry_string {
    my ( $self, $entry ) = @_;

    $self->bucket->delete_key($self->entry_key($entry));
    $self->bucket->add_key( $self->entry_key($entry), scalar($entry->slurp), $self->entry_headers($entry) );
}

sub entry_headers {
    my ( $self, $entry ) = @_;

    return {
        $self->entry_headers_etag($entry),
        $self->entry_headers_cache($entry),
        $self->entry_headers_extra($entry),
        %{ $entry->headers },
    };
}

sub entry_headers_etag {
    my ( $self, $entry ) = @_;

    return ( ETag => $entry->key ),
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

sub entry_headers_extra {
    my ( $self, $entry ) = @_;

    %{ $self->extra_headers }
}

__PACKAGE__->meta->make_immutable;

__PACKAGE__

__END__
