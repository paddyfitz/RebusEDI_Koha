package Rebus::EDI;

# Copyright 2012 Mark Gavillet

use strict;
use warnings;

=head1 NAME

Rebus::EDI

=head1 VERSION

Version 0.01

=cut

our $VERSION='0.01';

our @vendors	=	(
	{	
		name	=>	"Bertrams",
		san		=>	"0143731",
		ean		=>	"",
		module	=>	"Bertrams"
	},
	{	
		name	=>	"Bertrams",
		san		=>	"5013546025078",
		ean		=>	"",
		module	=>	"Bertrams"
	},
	{	
		name	=>	"Dawsons",
		san		=>	"",
		ean		=>	"5013546027856",
		module	=>	"Dawsons"
	},
	{	name	=>	"PTFS Europe",
		san		=>	"",
		ean		=>	"5011234567890",
		module	=>	"Default"
	},
);

sub new {
	my $class			=	shift;
	my $system			=	shift;
	my $self			=	{};
	$self->{system}		=	'koha';
	use Rebus::EDI::System::Koha;
	$self->{edi_system}	=	Rebus::EDI::System::Koha->new();
	bless $self, $class;
	return $self;
}

sub list_vendors {
	return @vendors;
}

sub retrieve_quotes {
	my $self					= shift;
	my @vendor_ftp_accounts		= $self->{edi_system}->retrieve_vendor_ftp_accounts;
	my @downloaded_quotes		= $self->{edi_system}->download_quotes(\@vendor_ftp_accounts);
	my $processed_quotes		= $self->{edi_system}->process_quotes(\@downloaded_quotes);
}

sub send_orders {
	my ($self,$order_id,$ean)	= @_;
	my $orders					= $self->{edi_system}->retrieve_orders($order_id);
	my $order_details			= $self->{edi_system}->retrieve_order_details($orders,$ean);
	foreach my $order (@{$order_details})
	{
		my $module=$order->{module};
		require "Rebus/EDI/Vendor/$module.pm";
		$module="Rebus::EDI::Vendor::$module";
		import $module;
		my $vendor_module=$module->new();
		my $order_message=$vendor_module->create_order_message($order);
		my $order_file=$self->{edi_system}->create_order_file($order_message,$order->{order_id});
	}
}

sub string35escape {
	my $string=shift;
	my $section;
	my $colon_string;
	my @sections;
	if (length($string)>35)
	{
		my ($chunk,$stringlength)=(35,length($string));
		for (my $counter=0;$counter<$stringlength;$counter+=$chunk)
		{
			push @sections,substr($string,$counter,$chunk);
		}
		foreach $section (@sections)
		{
			$colon_string.=$section.":";
		}
		chop($colon_string);
	}
	else
	{
		$colon_string=$string;
	}
	return $colon_string;
}

sub escape_reserved {
	my $string=shift;
	if ($string ne "")
	{
		$string=~ s/\?/\?\?/g;
		$string=~ s/\'/\?\'/g;
		$string=~ s/\:/\?\:/g;
		$string=~ s/\+/\?\+/g;
		return $string;
	}
	else
	{
		return;
	}
}

sub cleanisbn
{
	my $isbn=shift;
	if ($isbn ne "")
	{
		my $i=index($isbn,'(');
		if ($i>1)
		{
			$isbn=substr($isbn,0,($i-1));
		}
		if (index($isbn,"|") !=-1)
		{
			my @isbns=split(/\|/,$isbn);
			$isbn=$isbns[0];
		}
		$isbn=__PACKAGE__->escape_reserved($isbn);
		$isbn =~ s/^\s+//;
		$isbn =~ s/\s+$//;
		return $isbn;
	}
	else
	{
		return undef;
	}
}

1;