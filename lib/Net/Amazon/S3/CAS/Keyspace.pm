package Net::Amazon::S3::CAS::Keyspace;
use Moose::Role;

use namespace::clean -except => 'meta';

requires qw(
    blob_to_key
);

__PACKAGE__

__END__
