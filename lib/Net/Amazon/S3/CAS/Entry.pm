package Net::Amazon::S3::CAS::Entry;
use Moose;

with qw(MooseX::Clone);

use namespace::clean -except => 'meta';

has key => (
    isa      => "Str",
    is       => "ro",
    required => 1,
);

has blob => (
    does     => "Net::Amazon::S3::CAS::BLOB",
    is       => "ro",
    required => 1,
    handles  => "Net::Amazon::S3::CAS::BLOB",
);

has headers => (
    isa      => "HashRef",
    is       => "ro",
    required => 1,
    default  => sub { +{} },
);

__PACKAGE__->meta->make_immutable;

__PACKAGE__

__END__
