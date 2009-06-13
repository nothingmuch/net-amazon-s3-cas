package Net::Amazon::S3::CAS::Directory::Simple;
use Moose;

use MooseX::Types::Path::Class qw(Dir);

use Data::Stream::Bulk::Path::Class;

use Net::Amazon::S3::CAS::Entry;
use Net::Amazon::S3::CAS::BLOB::File;

use namespace::clean -except => 'meta';

with qw(Net::Amazon::S3::CAS::Directory);

has dir => (
    isa      => Dir,
    is       => "ro",
    required => 1,
    coerce   => 1,
);

has file_filter => (
    isa => "CodeRef|Str",
    is  => "ro",
    default => sub { sub { 1 } },
);

has keyspace => (
    does     => "Net::Amazon::S3::CAS::Keyspace",
    is       => "ro",
    handles  => "Net::Amazon::S3::CAS::Keyspace",
    default  => sub {
        require Net::Amazon::S3::CAS::Keyspace::Hash;
        Net::Amazon::S3::CAS::Keyspace::Hash->new;
    }
);

sub file_stream {
    my $self = shift;

    Data::Stream::Bulk::Path::Class->new(
        dir => $self->dir,
        only_files => 1,
    );
}

sub entries {
    my $self = shift;

    my $filter = $self->file_filter;

    $self->file_stream->filter(sub {[ map { $self->file_to_entry($_) } grep { $_->$filter } @$_ ]});
}

sub file_to_entry {
    my ( $self, $file ) = @_;

    my $blob = Net::Amazon::S3::CAS::BLOB::File->new( file => $file );

    Net::Amazon::S3::CAS::Entry->new(
        blob    => $blob,
        key     => $self->blob_to_key($blob),
        name    => $file->stringify,
    );
}

__PACKAGE__->meta->make_immutable;

__PACKAGE__

__END__
