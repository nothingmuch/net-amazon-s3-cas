package Net::Amazon::S3::CAS::Collection::Simple;
use Moose;

use Data::Stream::Bulk::Array;

use namespace::clean -except => 'meta';

has entries => (
    isa => "ArrayRef[Net::Amazon::S3::CAS::Entry]",
    reader => "_entries",
    required => 1,
);

sub entries {
    my $self = shift;
    
    Data::Stream::Bulk::Array->new( array => $self->_entries );
}

with qw(Net::Amazon::S3::CAS::Collection);

__PACKAGE__->meta->make_immutable;

__PACKAGE__

__END__
