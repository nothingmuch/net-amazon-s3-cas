package Net::Amazon::S3::CAS::BLOB::String;
use Moose;

use autodie;

use namespace::clean -except => 'meta';

with qw(Net::Amazon::S3::CAS::BLOB);

has data => (
    isa => "Str",
    is  => "ro",
    required => 1,
);

has name => (
    isa => "Str",
    is  => "ro",
    reader    => "_name",
    predicate => "_has_name",
);

# fucking roles
sub name { shift->_name }
sub has_name { shift->_has_name }

sub size {
    my $self = shift;
    length($self->data);
}

sub slurp { shift->data }

sub openr {
    my $self = shift;

    my $buf = $self->data;

    open my $fh, "<", \$buf;

    return $fh;
}

__PACKAGE__->meta->make_immutable;

__PACKAGE__

__END__
