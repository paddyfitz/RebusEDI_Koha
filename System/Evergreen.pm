package RebusEDI::System::Evergreen;

# Copyright 2011 Mark Gavillet

use strict;
use warnings;

=head1 NAME

RebusEDI::System::Evergreen

=head1 VERSION

Version 0.01

=cut

our $VERSION='0.01';
my $defaults = {
	"account=i"  => 0,
	"provider=i" => 0,
	"inactive"   => 0,
	"test"       => 0,
};
my $core			=	OpenILS::Utils::Cronscript->new($defaults);
our $e				=	$core->editor();
our $ftplogfile		=	'/tmp/edi_ftp.log';
our $quoteerrorlogfile	=	'/tmp/edi_quote_error.log';
our $edi_quote_user	=	143172;
our $host			=	"http://localhost/xml-rpc/";
our $db_name='evergreen';
our $db_port='5432';
our $db_user='evergreen';
our $db_pass='evergreen';

sub new {
	my $class			=	shift;
	my $self			=	{};
	bless $self, $class;
	return $self;
}

sub retrieve_vendor_ftp_accounts {
	my $self	= shift;
	use OpenILS::Utils::Cronscript;
	my $set		= __PACKAGE__->vendor_search($e) or die "No EDI accounts found in database (table: acq.edi_account)";
	my @accounts;
	my $new_account;
	foreach my $account (@$set)
	{
		$new_account	=	{
			account_id		=>	$account->[9]->[0],
			edi_account_id	=>	$account->[0],
			vendor			=>	$account->[1],
			server			=>	$account->[2],
			ftpuser			=>	$account->[3],
			ftppass			=>	$account->[4],
			ftpdir			=>	$account->[10],
			po_org_unit		=>	$account->[9]->[2],
		};
		push (@accounts,$new_account);
	}
	return @accounts;
}

sub vendor_search {
	my $self	= shift;
	my $e		= shift;
    my $select = {'+acqpro' => {active => {"in"=>['t','f']}} }; # either way
    my %args = @_ ? @_ : ();
    foreach (keys %args) {
        $select->{$_} = $args{$_};
    }
    return $e->search_acq_edi_account([
        $select,
        {
            'join' => 'acqpro',
            flesh => 1,
            flesh_fields => {acqedi => ['provider']},
        }
    ]);
}

sub download_quotes {
	my ($self,$ftp_accounts)=@_;
	my @local_files;
	foreach my $account (@$ftp_accounts) {	
		#get vendor details
		print "server: ".$account->{server}."\n";
		print "account: ".$account->{vendor}."\n";
		
		#get files
		use Net::FTP;
		my $newerr;
		my @ERRORS;
		my @files;
		open(EDIFTPLOG,">>$ftplogfile") or die "Could not open $ftplogfile\n";
		my ($sec,$min,$hour,$mday,$mon,$year) = localtime(time);
		printf EDIFTPLOG "\n\n%4d-%02d-%02d %02d:%02d:%02d\n-----\n",$year+1900,$mon+1,$mday,$hour,$min,$sec;
		print EDIFTPLOG "Connecting to ".$account->{server}."... ";
		my $ftp=Net::FTP->new($account->{server},Timeout=>10) or $newerr=1;
		push @ERRORS, "Can't ftp to ".$account->{server}.": $!\n" if $newerr;
		myerr(@ERRORS) if $newerr;
		if (!$newerr)
		{
			$newerr=0;
			print EDIFTPLOG "connected.\n";

			$ftp->login($account->{ftpuser},$account->{ftppass}) or $newerr=1;
			print EDIFTPLOG "Getting file list\n";
			push @ERRORS, "Can't login to ".$account->{server}.": $!\n" if $newerr;
			$ftp->quit if $newerr;
			myerr(@ERRORS) if $newerr; 
			if (!$newerr)
			{
				print EDIFTPLOG "Logged in\n";
				$ftp->cwd($account->{ftpdir}) or $newerr=1; 
				push @ERRORS, "Can't cd in server ".$account->{server}." $!\n" if $newerr;
				myerr(@ERRORS) if $newerr;
				$ftp->quit if $newerr;

					@files=$ftp->ls or $newerr=1;
					push @ERRORS, "Can't get file list from server ".$account->{server}." $!\n" if $newerr;
					myerr(@ERRORS) if $newerr;
					if (!$newerr)
					{
						print EDIFTPLOG "Got  file list\n";   
						foreach(@files) {
							my $filename=$_;
							if ((index lc($filename),'.ceq') > -1)
							{
								my $description = sprintf "%s/%s", $account->{server}, $filename;
								print EDIFTPLOG "Found file: $description - ";
								
								# deduplicate vs. acct/filenames already in DB
           						my $hits=find_duplicate_quotes($account->{edi_account_id},$account->{ftpdir},$filename);
           						
           						#my $hits = $e->search_acq_edi_message([
                				#	{
                				#		account     => $edi_account_id,
                				#		remote_file => $ftpdir."/".$_,
             					#		status      => {'in' => [qw/ processed /]},     # if it never got processed, go ahead and get the new one (try again)
                				#	}
            					#]);
            					
								my $match=0;
            					if (scalar(@$hits)) {
                					print EDIFTPLOG "File already retrieved. Skipping.\n";
                					$match=1;
            					}
								if ($match ne 1)
								{
									chdir "/tmp";
									$ftp->get($filename) or $newerr=1;
									push @ERRORS, "Can't transfer file ($filename) from ".$account->{server}." $!\n" if $newerr;
									$ftp->quit if $newerr;
									myerr(@ERRORS) if $newerr;
									if (!$newerr)
									{
										print EDIFTPLOG "File retrieved\n";
										open FILE,"/tmp/$filename" or die "Couldn't open file: $!\n";
										my $message_content=join("",<FILE>);
										close FILE;
										my $logged_quote=LogQuote($message_content, $account->{ftpdir}."/".$filename, $account->{server}, $account->{edi_account_id});
										my $quote_file	=	{
											filename	=>	$filename,
											account_id	=>	$account->{account_id},
											po_org_unit	=>	$account->{po_org_unit},
											edi_quote_user	=>	$edi_quote_user,
											logged_quote_id	=>	$logged_quote->id,
											edi_account_id	=>	$account->{edi_account_id},
										};
										push (@local_files,$quote_file);
										#my $ediparse=ParseEDIQuote($_,$account_id,$po_org_unit,$edi_quote_user,$e,$logged_quote->id,$edi_account_id);
										
									}
								}
							}
						}
					}
			}

			$ftp->quit;
		}
		$newerr=0;
	}
	return @local_files;
}

sub myerr {
	my @ERRORS= shift;
	open(EDIFTPLOG,">>$ftplogfile") or die "Could not open $ftplogfile\n";
	print EDIFTPLOG "Error: ";
	print EDIFTPLOG @ERRORS;
	close EDIFTPLOG;
}

sub find_duplicate_quotes {
	my ($edi_account_id, $ftpdir, $filename)	= @_;
	my $hits = $e->search_acq_edi_message([
	{
		account     => $edi_account_id,
		remote_file => $ftpdir."/".$filename,
		status      => {'in' => [qw/ processed /]},     # if it never got processed, go ahead and get the new one (try again)
	}
	]);
	return $hits;
}

# updates last activity in acq.edi_account and writes a new entry to acq.edi_message
sub LogQuote {
	my $incoming = Fieldmapper::acq::edi_message->new;
    my ($content, $remote, $server, $account_or_id) = @_;
    $content or return;
    #$e ||= new_editor;
	my $account = record_activity( $account_or_id );
	
	$incoming->remote_file($remote);
    $incoming->account($account->id);
    $incoming->edi($content);
    $incoming->message_type(($content =~ /'UNH\+\w+\+(\S{6}):/) ? $1 : 'QUOTES');   # cheap sniffing, QUOTES fallback
    $e->xact_begin;
    $e->create_acq_edi_message($incoming);
    $e->xact_commit;
    return $incoming;
}

sub record_activity {
	my ($account_or_id) = @_;
	$account_or_id or return;
	#$e ||= new_editor();
	my $account = ref($account_or_id) ? $account_or_id : $e->retrieve_acq_edi_account($account_or_id);
	#$logger->info("EDI record_activity calling update_acq_edi_account");
	$account->last_activity('NOW') or return;
	$e->xact_begin;
	$e->update_acq_edi_account($account) or log_quote_error("EDI: in record_activity, update_acq_edi_account FAILED. Account ID: $account_or_id\n");
	$e->xact_commit;
	return $account;
}

sub process_quotes {
	my ($self, $quotes)	= @_;
	foreach my $quote (@$quotes)
	{
		my $vendor_account = $e->retrieve_acq_provider($quote->{account_id});
		#my $module;
		#use RebusEDI::Main;
		#my @vendor_list=RebusEDI::Main::list_vendors();
		#foreach my $vendor (@vendor_list)
		#{
		#	if ($vendor->{san} eq $vendor_account->san || $vendor->{ean} eq $vendor_account->san)
		#	{
		#		$module=$vendor->{module};
		#		last;
		#	}
		#}
		my $module=get_vendor_module($vendor_account->san);
		$module or return;
		#use lib "/openils/lib/perl5";
		require "RebusEDI/Vendor/$module.pm";
		$module="RebusEDI::Vendor::$module";
		import $module;
		my $vendor_module=$module->new();
		my @parsed_quote=$vendor_module->parse_quote($quote);
		my $order_id=create_order_from_quote(@parsed_quote,$quote->{filename},$quote->{account_id},$quote->{po_org_unit},$quote->{logged_quote_id});
		#create line items inc. line item detail
		foreach my $item (@parsed_quote)
		{
			my $li = Fieldmapper::acq::lineitem->new;
			### acq.lineitem
			my $author=$item->{author};
			my $title=$item->{title};
			my $isbn=$item->{isbn};
			my $price=$item->{price};
			my $publisher=$item->{publisher};
			my $year=$item->{year};
			my $item_reference=$item->{item_reference};
			my $eg_bib_id=isbn_search($isbn);
			$li->creator($edi_quote_user);
			$li->editor($edi_quote_user);
			$li->selector($edi_quote_user);
			$li->provider($quote->{account_id});
			$li->purchase_order($order_id);
			$li->create_time('NOW');
			$li->edit_time('NOW');
			$li->eg_bib_id($eg_bib_id);
			$li->state('new');
			$li->estimated_unit_price($price);
			$li->source_label($item_reference);
			my $marc_header='<record xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns="http://www.loc.gov/MARC21/slim" xmlns:marc="http://www.loc.gov/MARC21/slim" xsi:schemaLocation="http://www.loc.gov/MARC21/slim http://www.loc.gov/ standards/marcxml/schema/MARC21slim.xsd">';
			my $marc_leader='<leader>00000nam a22000007a 4500</leader>';
			my $marc_245='<marc:datafield tag="245" ind1=" " ind2=" "><marc:subfield code="a">'.$title.'</marc:subfield></marc:datafield>';
			my $marc_100='<marc:datafield tag="100" ind1=" " ind2=" "><marc:subfield code="a">'.$author.'</marc:subfield></marc:datafield>';
			my $marc_020='<marc:datafield tag="020" ind1=" " ind2=" "><marc:subfield code="a">'.$isbn.'</marc:subfield></marc:datafield><marc:datafield tag="020" ind1=" " ind2=" "><marc:subfield code="c">'.$price.'</marc:subfield></marc:datafield>';
			my $marc_260b='<marc:datafield tag="260" ind1=" " ind2=" "><marc:subfield code="b">'.$publisher.'</marc:subfield></marc:datafield>';
			my $marc_260c='<marc:datafield tag="260" ind1=" " ind2=" "><marc:subfield code="c">'.$year.'</marc:subfield></marc:datafield>';
			my $marc_footer='</record>';
			my $marc=$marc_header.$marc_leader.$marc_245.$marc_100.$marc_020.$marc_260b.$marc_260c.$marc_footer;
			$li->marc($marc);
			$e->xact_begin;
			my $new_li=$e->create_acq_lineitem($li) or log_quote_error("EDI: in process_quotes, create_acq_lineitem FAILED. Purchase order: $order_id ($title - $author : $isbn)\n");
			$e->xact_commit;
			my $li_id=$new_li->id;
			
			### acq.lineitem_detail
			foreach my $copies ($item->{copies})
			{
				my @copies=@$copies;
				my $i=0;
				for ($i;$i<scalar(@copies);$i++)
				{
					my $li_detail=Fieldmapper::acq::lineitem_detail->new;
					my $lst=$copies[$i]->{lst};
					my $shelfmark=$copies[$i]->{shelfmark};
					my $lfn=$copies[$i]->{lfn};
					my $note=$copies[$i]->{note};
					my $lsq=$copies[$i]->{lsq};
					my $llo=$copies[$i]->{llo};
					$li_detail->lineitem($li_id);
					$li_detail->fund(get_fund_id($lfn));
					$li_detail->cn_label($shelfmark);
					$li_detail->note($note);
					$li_detail->circ_modifier($lst);
					$li_detail->owning_lib(get_owning_lib_id($llo));
					$li_detail->location(get_copy_location_id($lsq));
					$e->xact_begin;
					my $new_li_detail=$e->create_acq_lineitem_detail($li_detail) or log_quote_error("EDI: in process_quotes, create_acq_lineitem_detail FAILED. Purchase order: $order_id ($title - $author : $isbn) Lineitem ID: $li_id\n");
					$e->xact_commit;
				}				
			}
		}
		### mark quote as processed
		my $logged_quote = ref($quote->{logged_quote_id}) ? $quote->{logged_quote_id} : $e->retrieve_acq_edi_message($quote->{logged_quote_id});
		$logged_quote->status('processed');
		$e->xact_begin;
		$e->update_acq_edi_message($logged_quote) or log_quote_error("EDI: in process_quotes, update_acq_edi_message FAILED when updating status to processed. EDI message ID: ".$quote->{logged_quote_id}."\n");
		$e->xact_commit;
		### manipulate quote file on remote server
		my $vendor_ftp_account=get_vendor_ftp_account_by_order_id($order_id);
		$vendor_module->post_process_quote_file($logged_quote->remote_file,$vendor_ftp_account);
	}
}

sub create_order_from_quote {
	my ($self,$parsed_quote,$filename,$account_id,$po_org_unit,$logged_quote_id)	=	@_;
	my $po = Fieldmapper::acq::purchase_order->new;
	$po->owner($edi_quote_user);
	$po->creator($edi_quote_user);
	$po->editor($edi_quote_user);
	$po->ordering_agency($po_org_unit);
	$po->provider($account_id);
	$po->name($filename);
	$po->state('pending');
	$e->xact_begin;
    $e->create_acq_purchase_order($po) or log_quote_error("EDI: in create_order_from_quote, create_acq_purchase_order FAILED. Filename: $filename (Account ID: $account_id)\n");
    $e->xact_commit;
    #return $po;
    # update acq.edi_message
    my $logged_quote = ref($logged_quote_id) ? $logged_quote_id : $e->retrieve_acq_edi_message($logged_quote_id);
    $logged_quote->purchase_order($po->id);
    $e->xact_begin;
    $e->update_acq_edi_message($logged_quote) or log_quote_error("EDI: in create_order_from_quote, update_acq_edi_message FAILED. EDI message ID: $logged_quote\n");
    $e->xact_commit;
    return $po->id;
}

sub isbn_search {
	my $isbn=shift;
	my $response= request(
		'open-ils.search',
		'open-ils.search.biblio.isbn.staff',
		$isbn )->value;
	die "No search response returned\n" unless $response;
	if ($response->{count}>0)
	{
		return $response->{ids}[0];
	}
	else
	{
		return;
	}
}

sub request {
	my( $service, $method, @args ) = @_;
	use RPC::XML::Client;
	use RPC::XML qw/smart_encode/;
	my $connection = RPC::XML::Client->new("$host/$service");
	my $resp = $connection->send_request($method, smart_encode(@args));
	return $resp;
}

sub get_fund_id {
	my $fundcode=shift;
	my $fund = $e->search_acq_fund({code => $fundcode});
    return $fund->[0]->[0];
}

sub get_owning_lib_id {
	my $libcode=shift;
	my $lib = $e->search_actor_org_unit({shortname => $libcode});
	return $lib->[0]->[3];
}

sub get_copy_location_id {
	my $loccode=shift;
	my $loc = $e->search_asset_copy_location({name => $loccode});
    return $loc->[0]->[3];
}

sub log_quote_error {
	my $error=shift;
	open(ERRORLOG,">>$quoteerrorlogfile") or die "Could not open $quoteerrorlogfile\n";
	print ERRORLOG $error;
	close ERRORLOG;
}

sub retrieve_orders {
	my $self	= shift;
	use DBI;
	my $dbh = DBI->connect("DBI:Pg:dbname=$db_name;port=$db_port", "$db_user", "$db_pass"
	           ) || die "Could not connect to database: $DBI::errstr";
	           
	## retrieve on-order purchase order ids for vendors with an edi account
	my $sth=$dbh->prepare('select acq.purchase_order.id,acq.purchase_order.provider from acq.purchase_order, acq.edi_account 
		where acq.purchase_order.provider=acq.edi_account.provider and acq.purchase_order.state=?');
	$sth->execute('on-order');
	my $orders = $sth->fetchall_arrayref( {} );
	
	## check existing edi order messages
	my @active_orders;
	foreach my $order (@{$orders})
	{
		my $sth=$dbh->prepare('select id,status from acq.edi_message where message_type=? and 
			purchase_order=? and status=?');
		$sth->execute('ORDERS',$order->{id},'complete');
		my @message;
		if ($sth->rows==0)
		{
			push @active_orders,{order_id=>$order->{id},provider_id=>$order->{provider}};
		}
	}
	#use Data::Dumper;print Dumper(@active_orders);
	return \@active_orders;
}

sub retrieve_order_details {
	my ($self,$orders)	= @_;
	#use Data::Dumper;print "details: ".Dumper($orders);
	my @fleshed_orders;
	foreach my $order (@{$orders})
	{
		my $fleshed_order;
		#print "oid: ".$order->{order_id}."\n";
		$fleshed_order={order_id=>$order->{order_id},provider_id=>$order->{provider_id}};
		
		## retrieve module for vendor
		use DBI;
		my $dbh = DBI->connect("DBI:Pg:dbname=$db_name;port=$db_port", "$db_user", "$db_pass"
	           ) || die "Could not connect to database: $DBI::errstr";
	    my $sth=$dbh->prepare('select san from acq.provider where id=?');
		$sth->execute($order->{provider_id});
		my @result;
		my $san;
		while (@result = $sth->fetchrow_array())
		{
			$san=$result[0];
		}
		$fleshed_order->{'module'}=get_vendor_module($san);
		$fleshed_order->{'san_or_ean'}=$san;
		$fleshed_order->{'org_san'}=get_org_san($order->{order_id});
		$fleshed_order->{'quote_or_order'}=quote_or_order($order->{order_id});
		my @lineitems=get_order_lineitems($order->{order_id});
		$fleshed_order->{'lineitems'}=\@lineitems;
		
		push @fleshed_orders,$fleshed_order;
	}
	#use Data::Dumper;print Dumper(@fleshed_orders);
	return \@fleshed_orders;
}

sub get_order_lineitems {
	my $order_id=shift;
	my @lineitems;
	my $dbh = DBI->connect("DBI:Pg:dbname=$db_name;port=$db_port", "$db_user", "$db_pass"
			) || die "Could not connect to database: $DBI::errstr";
	my $sth=$dbh->prepare('select id,marc,source_label from acq.lineitem where purchase_order=?');
	$sth->execute($order_id);
	my $lineitems = $sth->fetchall_arrayref( {} );
	foreach my $lineitem (@{$lineitems})
	{
		my $marc				=	$lineitem->{marc};
		my $fleshed_lineitem	=	lineitem_from_marc($marc);
		#use Data::Dumper; print "fleshed_lineitem: ".Dumper($fleshed_lineitem);
		#while ((my $key, my $value) = each %$lineitem_from_marc)
		#{
			#print "key: $key - value: $value\n";
		#	$lineitem->{$key}=$value;
		#}
		$fleshed_lineitem->{binding}	=	'O';
		$fleshed_lineitem->{currency}	=	'GBP';
		$fleshed_lineitem->{id}			=	$lineitem->{id};
		$fleshed_lineitem->{qli}		=	$lineitem->{source_label};
		$fleshed_lineitem->{rff}		=	$order_id."/".$fleshed_lineitem->{id};
		my @lineitem_copies;
		my $detail_sth=$dbh->prepare('select id,fund,cn_label,note,circ_modifier,owning_lib,location 
			from acq.lineitem_detail where lineitem=?');
		#use Data::Dumper;print "lineitem_id: ".$fleshed_lineitem->{id}."\n";
		$detail_sth->execute($fleshed_lineitem->{id});
		$fleshed_lineitem->{quantity}	=	$detail_sth->rows;
		#use Data::Dumper;print "fleshed_lineitem: ".Dumper($fleshed_lineitem);
		my $lineitem_details = $detail_sth->fetchall_arrayref( {} );
		foreach my $lineitem_detail (@{$lineitem_details})
		{
			my $fleshed_lineitem_detail;
			#branchcode llo
			$fleshed_lineitem_detail->{llo}		=	get_branchcode_from_id($lineitem_detail->{owning_lib});
			
			#fundcode lfn
			$fleshed_lineitem_detail->{lfn}		=	get_fundcode_from_id($lineitem_detail->{fund});
			
			#sequence lsq asset.copy_location
			$fleshed_lineitem_detail->{lsq}		=	get_location_from_id($lineitem_detail->{location});
			
			$fleshed_lineitem_detail->{lst}		=	$lineitem_detail->{circ_modifier};
			$fleshed_lineitem_detail->{note}	=	$lineitem_detail->{note};
			$fleshed_lineitem_detail->{lcl}		=	$lineitem_detail->{cn_label};
			
			#use Data::Dumper;print "detail: ".Dumper($fleshed_lineitem_detail);
			push (@lineitem_copies,$fleshed_lineitem_detail);
		}
		#use Data::Dumper;print Dumper(@lineitem_copies);
		$fleshed_lineitem->{copies}		=	\@lineitem_copies;
		#use Data::Dumper;print Dumper($fleshed_lineitem->{copies});
		push (@lineitems,$fleshed_lineitem);
		#use Data::Dumper;print Dumper($fleshed_lineitem);
	}
	#use Data::Dumper;print Dumper(@lineitems);
	return @lineitems;
}

sub get_branchcode_from_id {
	my $branch_id=shift;
	my @result;
	my $llo;
	my $dbh = DBI->connect("DBI:Pg:dbname=$db_name;port=$db_port", "$db_user", "$db_pass"
			) || die "Could not connect to database: $DBI::errstr";
	my $sth=$dbh->prepare('select shortname from actor.org_unit where id=?');
	$sth->execute($branch_id);
	while (@result = $sth->fetchrow_array())
	{
		$llo=$result[0];
	}
	return $llo;
}

sub get_fundcode_from_id {
	my $fund_id=shift;
	my @result;
	my $lfn;
	my $dbh = DBI->connect("DBI:Pg:dbname=$db_name;port=$db_port", "$db_user", "$db_pass"
			) || die "Could not connect to database: $DBI::errstr";
	my $sth=$dbh->prepare('select code from acq.fund where id=?');
	$sth->execute($fund_id);
	while (@result = $sth->fetchrow_array())
	{
		$lfn=$result[0];
	}
	return $lfn;
}

sub get_location_from_id {
	my $loc_id=shift;
	my @result;
	my $lsq;
	my $dbh = DBI->connect("DBI:Pg:dbname=$db_name;port=$db_port", "$db_user", "$db_pass"
			) || die "Could not connect to database: $DBI::errstr";
	my $sth=$dbh->prepare('select name from asset.copy_location where id=?');
	$sth->execute($loc_id);
	while (@result = $sth->fetchrow_array())
	{
		$lsq=$result[0];
	}
	return $lsq;
}

sub lineitem_from_marc {
	my $marc=shift;
	my $lineitem_from_marc={};
	use XML::Simple;
	my $xml=new XML::Simple;
	my $data=$xml->XMLin($marc);
	foreach my $field (@{$data->{'marc:datafield'}})
	{
		#isbn
		if ($field->{tag} eq '020' && $field->{'marc:subfield'}->{code} eq 'a')
		{
			$lineitem_from_marc->{isbn}=$field->{'marc:subfield'}->{content};
		}
		#title
		if ($field->{tag} eq '245' && $field->{'marc:subfield'}->{code} eq 'a')
		{
			$lineitem_from_marc->{title}=$field->{'marc:subfield'}->{content};
		}
		#author
		if ($field->{tag} eq '100' && $field->{'marc:subfield'}->{code} eq 'a')
		{
			$lineitem_from_marc->{author}=$field->{'marc:subfield'}->{content};
		}
		#publisher
		if ($field->{tag} eq '260' && $field->{'marc:subfield'}->{code} eq 'b')
		{
			$lineitem_from_marc->{publisher}=$field->{'marc:subfield'}->{content};
		}
		#date of publication
		if ($field->{tag} eq '260' && $field->{'marc:subfield'}->{code} eq 'c')
		{
			$lineitem_from_marc->{year}=$field->{'marc:subfield'}->{content};
		}
		#price
		if ($field->{tag} eq '020' && $field->{'marc:subfield'}->{code} eq 'c')
		{
			$lineitem_from_marc->{price}=$field->{'marc:subfield'}->{content};
		}
	}
	return $lineitem_from_marc;
}

sub get_vendor_module {
	my $san=shift;
	my $module;
	use RebusEDI::Main;
	my @vendor_list=RebusEDI::Main::list_vendors();
	foreach my $vendor (@vendor_list)
	{
		if ($san eq $vendor->{san} || $san eq $vendor->{ean})
		{
			$module=$vendor->{module};
			last;
		}
	}
	return $module;
}

sub get_org_san {
	my $order_id=shift;
	my $org_id;
	my $org_san;
	my @result;
	my $dbh = DBI->connect("DBI:Pg:dbname=$db_name;port=$db_port", "$db_user", "$db_pass"
	           ) || die "Could not connect to database: $DBI::errstr";
	my $sth=$dbh->prepare('select actor.org_unit.id from actor.org_unit inner join 
		acq.purchase_order on actor.org_unit.id=acq.purchase_order.ordering_agency 
		where acq.purchase_order.id=?');
	$sth->execute($order_id);
	while (@result = $sth->fetchrow_array())
	{
		$org_id=$result[0];
	}
	$sth=$dbh->prepare('select actor.org_address.san from actor.org_address, 
		actor.org_unit where actor.org_address.id=actor.org_unit.mailing_address 
		and actor.org_unit.id=?');
	$sth->execute($org_id);
	while (@result = $sth->fetchrow_array())
	{
		$org_san=$result[0];
	}
	return $org_san;
}

sub quote_or_order {
	my $order_id=shift;
	my @result;
	my $quote_or_order;
	my $dbh = DBI->connect("DBI:Pg:dbname=$db_name;port=$db_port", "$db_user", "$db_pass"
	           ) || die "Could not connect to database: $DBI::errstr";
	my $sth=$dbh->prepare('select id from acq.edi_message where purchase_order=? and 
		message_type=?');
	$sth->execute($order_id,'QUOTES');
	if ($sth->rows==0)
	{
		$quote_or_order='o';
	}
	else
	{
		$quote_or_order='q';
	}
	return $quote_or_order;
}

sub create_order_file {
	my ($self,$order_message,$order_id)=@_;
	#print "\n*$order_id*\n";
	my $filename="/tmp/ediorder_$order_id.CEP";
	open(EDIORDER,">$filename");
	print EDIORDER $order_message;
	close EDIORDER;
	my $vendor_ftp_account=get_vendor_ftp_account_by_order_id($order_id);
	#use Data::Dumper;print Dumper($vendor_ftp_account);
	my $sent_order=send_order_message($filename,$vendor_ftp_account,$order_message,$order_id);
	#my $sent_order=$self->{edi_system}->send_order_message($order_file);
	return $filename;
}

sub get_vendor_ftp_account_by_order_id {
	my $order_id=shift;
	my $vendor_ftp_account;
	my @result;
	my $dbh = DBI->connect("DBI:Pg:dbname=$db_name;port=$db_port", "$db_user", "$db_pass"
	           ) || die "Could not connect to database: $DBI::errstr";
	my $sth=$dbh->prepare('select acq.edi_account.* from acq.edi_account, acq.purchase_order 
		where acq.edi_account.provider=acq.purchase_order.provider and acq.purchase_order.id=?');
	$sth->execute($order_id);
	while (@result = $sth->fetchrow_array())
	{
		$vendor_ftp_account->{id}				=	$result[0];
		$vendor_ftp_account->{label}			=	$result[1];
		$vendor_ftp_account->{host}				=	$result[2];
		$vendor_ftp_account->{username}			=	$result[3];
		$vendor_ftp_account->{password}			=	$result[4];
		$vendor_ftp_account->{account}			=	$result[5];
		$vendor_ftp_account->{path}				=	$result[6];
		$vendor_ftp_account->{owner}			=	$result[7];
		$vendor_ftp_account->{last_activity}	=	$result[8];
		$vendor_ftp_account->{provider}			=	$result[9];
		$vendor_ftp_account->{in_dir}			=	$result[10];
		$vendor_ftp_account->{vendcode}			=	$result[11];
		$vendor_ftp_account->{vendacct}			=	$result[12];
	}
	$dbh = DBI->connect("DBI:Pg:dbname=$db_name;port=$db_port", "$db_user", "$db_pass"
	           ) || die "Could not connect to database: $DBI::errstr";
	$sth=$dbh->prepare('select id from acq.edi_account where provider=?');
	$sth->execute($vendor_ftp_account->{provider});
	while (@result = $sth->fetchrow_array())
	{
		$vendor_ftp_account->{edi_account_id}	=	$result[0];
	}
	return $vendor_ftp_account;
}

sub send_order_message {
	my ($filename,$ftpaccount,$order_message,$order_id)=@_;
	my ($basketno,$booksellerid) = @_;
	my @ERRORS;
	my $newerr;
	my $result;
	
	open(EDIFTPLOG,">>$ftplogfile") or die "Could not open $ftplogfile\n";
		my ($sec,$min,$hour,$mday,$mon,$year) = localtime(time);
		printf EDIFTPLOG "\n\n%4d-%02d-%02d %02d:%02d:%02d\n-----\n",$year+1900,$mon+1,$mday,$hour,$min,$sec;
		
	# check edi order file exists
	if (-e $filename)
	{
		use Net::FTP;
		
		print EDIFTPLOG "Connecting to ".$ftpaccount->{host}."... ";
		# connect to ftp account
		my $ftp=Net::FTP->new($ftpaccount->{host},Timeout=>10) or $newerr=1;
		push @ERRORS, "Can't ftp to ".$ftpaccount->{host}.": $!\n" if $newerr;
		myerr(@ERRORS) if $newerr;
		if (!$newerr)
		{
			$newerr=0;
			print EDIFTPLOG "connected.\n";
			
			# login
			$ftp->login("$ftpaccount->{username}","$ftpaccount->{password}") or $newerr=1;
			$ftp->quit if $newerr;
			print EDIFTPLOG "Logging in...\n";
			push @ERRORS, "Can't login to ".$ftpaccount->{host}.": $!\n" if $newerr;
			myerr(@ERRORS) if $newerr;
			if (!$newerr)
			{
				print EDIFTPLOG "Logged in\n";
				# cd to directory
				$ftp->cwd("$ftpaccount->{path}") or $newerr=1; 
				push @ERRORS, "Can't cd in server ".$ftpaccount->{host}." $!\n" if $newerr;
				myerr(@ERRORS) if $newerr;
				$ftp->quit if $newerr;
				
				# put file
				if (!$newerr)
				{
					$newerr=0;
   					$ftp->put($filename) or $newerr=1;
   					push @ERRORS, "Can't write order file to server ".$ftpaccount->{host}." $!\n" if $newerr;
					myerr(@ERRORS) if $newerr;
					$ftp->quit if $newerr;
   					if (!$newerr)
   					{
   						print EDIFTPLOG "File: $filename transferred successfully\n";
   						$ftp->quit;
   						unlink($filename);
   						record_activity($ftpaccount->{id});
   						log_order($order_message,$ftpaccount->{path}.substr($filename,4),$ftpaccount->{edi_account_id},$order_id);
   						
   						return $result;
   					}
   				}			
			}			
		}
	}
	else
	{
		print EDIFTPLOG "Order file $filename does not exist\n";
	}
}

sub log_order {
	my $outgoing = Fieldmapper::acq::edi_message->new;
	#use Data::Dumper;print Dumper($e);
    my ($content, $remote, $edi_account_id, $order_id) = @_;
	
	$outgoing->remote_file($remote);
    $outgoing->account($edi_account_id);
    $outgoing->edi($content);
    $outgoing->message_type('ORDERS');
    $outgoing->status('complete');
    $outgoing->purchase_order($order_id);
    #use Data::Dumper;print Dumper($outgoing);
    $e->xact_begin;
    $e->create_acq_edi_message($outgoing) or die "couldn't create edi_message";
    $e->xact_commit;
    return $outgoing;
}

1;