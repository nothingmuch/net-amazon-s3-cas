package Net::Amazon::S3::CAS::BLOB;
use Moose::Role;

use namespace::clean -except => 'meta';

requires qw(
    size
    openr
    slurp
);

sub prefer_handle { 0 }

sub has_name { "" }
sub name { undef }

__PACKAGE__

__END__
