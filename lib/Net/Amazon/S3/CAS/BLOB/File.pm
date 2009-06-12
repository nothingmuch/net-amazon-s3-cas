package Net::Amazon::S3::CAS::BLOB::File;
use Moose;

use MooseX::Types::Path::Class qw(File);

use namespace::clean -except => 'meta';

has file => (
    isa      => File,
    is       => "ro",
    required => 1,
    coerce   => 1,
    handles  => {
        slurp    => "slurp",
        openr    => "openr",
        filename => "stringify",
    },
);

has size => (
    isa => "Int",
    is => "ro",
    lazy_build => 1,
);

sub _build_size {
    my $self = shift;

    $self->file->stat->size;
}

sub prefer_handle { 1 }

sub name { shift->filename }

sub has_name { 1 }

with qw(Net::Amazon::S3::CAS::BLOB);

__PACKAGE__->meta->make_immutable;

__PACKAGE__

__END__
