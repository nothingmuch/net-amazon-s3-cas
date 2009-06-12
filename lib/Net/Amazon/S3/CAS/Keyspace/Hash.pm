package Net::Amazon::S3::CAS::Keyspace::Hash;
use Moose;

use Digest;
use File::Basename;

use namespace::clean -except => 'meta';

with qw(Net::Amazon::S3::CAS::Keyspace);

has digest_algorithm => (
    isa => "Str",
    is  => "ro",
    default => "SHA-1",
);

has include_name => (
    isa => "Bool",
    is  => "ro",
    default => 0,
);

has basename => (
    isa => "Bool",
    is  => "ro",
    default => 1,
);

has delimiter => (
    isa => "Str",
    is  => "ro",
    default => ".",
);

sub blob_to_key {
    my ( $self, $blob ) = @_;

    my $digest = $self->hash_blob($blob);

    my $digest_key = $self->digest_to_key($digest);

    $self->format_key( $blob, $digest_key );
}

sub format_key {
    my ( $self, $blob, $digest_key ) = @_;

    if ( $self->include_name && defined(my $name = $blob->name) ) {
        local $File::Basename::Fileparse_igncase = 1;
        my ( $basename, $path, $ext ) = fileparse($name, qr/\.(?:[^\.\s]+)/);
        
        return join $self->delimiter, ( $self->basename ? $basename : $name ), $digest_key . $ext;
    } else {
        return $digest_key;
    }
}

sub hash_blob {
    my ( $self, $blob ) = @_;

    my $digest = $self->new_digest($blob);

    $self->digest_add_blob($digest, $blob);

    return $digest;
}

sub new_digest {
    my ( $self, $blob ) = @_;

    Digest->new($self->digest_algorithm);
}

sub digest_add_blob {
    my ( $self, $digest, $blob ) = @_;

    if ( $blob->prefer_handle ) {
        $digest->addfile( $blob->openr );
    } else {
        $digest->add( $blob->slurp );
    }
}

sub digest_to_key {
    my ( $self, $digest ) = @_;

    $digest->hexdigest;
}

__PACKAGE__->meta->make_immutable;

__PACKAGE__

__END__
