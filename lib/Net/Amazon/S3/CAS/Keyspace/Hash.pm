package Net::Amazon::S3::CAS::Keyspace::Hash;
use Moose;

use Digest;

use namespace::clean -except => 'meta';

with qw(Net::Amazon::S3::CAS::Keyspace);

has digest_algorithm => (
    isa => "Str",
    is  => "ro",
    default => "SHA-1",
);

sub blob_to_key {
    my ( $self, $blob ) = @_;

    my $digest = $self->hash_blob($blob);

    $self->digest_to_key($digest);
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
