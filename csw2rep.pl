#!/usr/local/bin/perl

###############################################################################
# csw2rep.pl
#
# Fetches metadata from a CSW catalogue, parses response, tests links, reports.
# 
# version 0.9
# 2021-02-23
# 
# This program connects to a CSW catalog, downloads metadata posts as a XML file 
# and parses its contents. The program reports links to metadata, tests datset 
# and service links, and reports eventual fails.
# 
# The program is written in Perl 5 and requires a number of Perl modules.
# Runs on Linux. Some adaptation may be needed to work on other operative systems.
#
# Please, note that this program is NOT a polished, neatly defined and user-friendly 
# routine, but a set of tools to recursively explore, test and extract metadata. 
# Different program sections can be commented or uncommented achieve different goals.
# I hope the comments in the code are self-explanatory.
# 
# 
# Usage:
# perl csw2rep.pl scope
# where:
# scope = "dataset" or "service"
# 
# The following arguments can be hardcoded: 
# keyword = any desired keyword, i.e. "Inspire"
# organisation = organisation name
#
# Example:
# csw2rep.pl dataset
# for metadata records for dataset
# 
# Hernán De Angelis, GeoNatura AB
#
###############################################################################

# required pragmas
use warnings;
use strict;
use utf8;

# required modules
use Encode qw(decode encode);
use POSIX qw(strftime);
use LWP::UserAgent;
use XML::LibXML;
use XML::LibXML::XPathContext;
use List::Util qw(any uniq uniqstr);

# define scope
# read from command line 
my $scope = shift @ARGV;
# or hard-code
# my $scope = "dataset";
# my $scope = "service";

# define organisation
# read from command line 
# my $orgName = shift @ARGV;
# or hard-code
# my $orgName;
# my $orgName = "Naturvårdsverket";
my $orgName = "Havs- och vattenmyndigheten";

# define keyword
# read from command line 
# my $mdKwd = shift @ARGV;
# or hard-code
# my $mdKwd;
my $mdKwd = "Inspire";
# my $mdKwd = "Miljödataportalen";
# my $mdKwd = "Nationella Geodataportalen";

# define metadata UUID, for selective search
my $recordUUID;
# my $recordUUID = qw(94c363b1-68aa-42c0-a25d-35e236973afd);


# define CSW catalogue adress

# # NV CMDK
# my $catalogue = "https://metadatakatalogen.naturvardsverket.se/geonetwork/srv/eng/csw-inspire?";

# LM Geodataportalen
my $catalogue = "https://www.geodata.se/geodataportalen/srv/eng/csw-inspire?";

# # LST planeringskatalogen
# my $catalogue = qq(https://ext-geodatakatalog-forv.lansstyrelsen.se/geonetwork/srv/eng/csw?);


# get date
my $date = strftime "%Y%m%d", localtime;

# define metadata file
my $metadataFile = qq(metadata_$date.xml);

# define main report file
open (REP, '>', "metadata_$date-$scope.rep");

# define fail report file
open (FAIL, '>', "metadata_$date-$scope.fail");

# define auxiliary report file
open (AUX, '>', "metadata_$date-$scope.aux");
	
# for use with sorted lists, where title is key
my %midTEX;
my %didTEX;
my %kwdTEX;
my %lnkTEX;


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# Prepare request

# define elementSetName
my $elementSetName = "full";
# 	my $elementSetName = "brief";
    
# define maxRecords
my $maxRecords = 1000;

# define typeNames
# my $typeNames = 'csw:Record';
my $typeNames = 'gmd:MD_Metadata';

# declare empty variable for constraints
my $constraints = '';

# add keyword if given
if (defined $mdKwd) {
    $constraints = $constraints.qq(
                <PropertyIsEqualTo>
                    <PropertyName>keyword</PropertyName>
                    <Literal>$mdKwd</Literal>
                </PropertyIsEqualTo>);
    }

# add organisation if given
if (defined $orgName) {
    $constraints = $constraints.qq(
                <PropertyIsEqualTo>
                    <PropertyName>organisationName</PropertyName>
                    <Literal>$orgName</Literal>
                </PropertyIsEqualTo>);
    }
    
# add record title if given
if (defined $recordUUID) {
    $constraints = $constraints.qq(
                <PropertyIsEqualTo>
                    <PropertyName>identifier</PropertyName>
                    <Literal>$recordUUID</Literal>
                </PropertyIsEqualTo>);
    }

# create POST request
my $cswRequest = qq(<?xml version="1.0" encoding="UTF-8"?><csw:GetRecords xmlns:csw="http://www.opengis.net/cat/csw/2.0.2" xmlns:gmd="http://www.isotc211.org/2005/gmd" xmlns:gml="http://www.opengis.net/gml" xmlns:ows="http://www.opengis.net/ows" xmlns="http://www.opengis.net/ogc" service="CSW" version="2.0.2" resultType="results" outputSchema="csw:IsoRecord" startPosition="1" maxRecords="$maxRecords">
<csw:Query typeNames="$typeNames">
    <csw:ElementSetName>$elementSetName</csw:ElementSetName>
    <csw:Constraint version="1.0.0">
        <Filter>
            <And>
                <!-- constraints -->
                $constraints
            </And>     
        </Filter>
    </csw:Constraint>
</csw:Query>
</csw:GetRecords>);

# encode request
utf8::encode($cswRequest);

# create user agent object
my $ua = LWP::UserAgent->new();
# prepare header
my $header = ['Content-type' => 'application/xml; charset=UTF-8'];
# post request
my $request = HTTP::Request->new('POST', $catalogue, $header, $cswRequest);
# get response
my $response = $ua->request($request);

#  save to file
open(MD, '>', $metadataFile);
my $decodedResponse = $response->decoded_content;
utf8::encode($decodedResponse);
print MD $decodedResponse;
close MD;


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# define DOM
my $dom = XML::LibXML->load_xml( location => $metadataFile, no_blanks => 1  );
my $xpc = XML::LibXML::XPathContext->new($dom);
$xpc->registerNs('csw', 'http://www.opengis.net/cat/csw/2.0.2');
$xpc->registerNs('xsi', 'http://www.w3.org/2001/XMLSchema-instance');
$xpc->registerNs('gmd', 'http://www.isotc211.org/2005/gmd');
$xpc->registerNs('srv', 'http://www.isotc211.org/2005/srv');
$xpc->registerNs('gco', 'http://www.isotc211.org/2005/gco');
$xpc->registerNs('xlink', 'http://www.w3.org/1999/xlink');
$xpc->registerNs('gts', 'http://www.isotc211.org/2005/gts');
$xpc->registerNs('gml', 'http://www.opengis.net/gml');
$xpc->registerNs('geonet', 'http://www.fao.org/geonetwork');

my @metadataUUID;
my @dataUUID;

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# start loop, iterate over each MD_Metadata element and its children
my %list;
my $count = 1;
my $countx = 1;

# open records file and parse
print "Parse and Test ... \n";

foreach my $metadataElement ($xpc->findnodes('//gmd:MD_Metadata')) {
#     print "$count","\n";

    # find UUID
    my $UUID  = $metadataElement->findvalue('./gmd:fileIdentifier');
    #     next if $UUID =~ /oai:DiVA.org:naturvardsverket/;
    $UUID =~ s/[\v\h\s]//g;
    # 	print $UUID,"\n";

	# find scope: dataset or service
    my $scopeCode = $metadataElement->findvalue('./gmd:dataQualityInfo/gmd:DQ_DataQuality/gmd:scope/gmd:DQ_Scope/gmd:level/gmd:MD_ScopeCode/@codeListValue');
#     print $scopeCode,"\n";

	# find title, different specifications for service or dataset
    my $title;
    if ($scopeCode eq "dataset") {
        $title = $metadataElement->findvalue('./gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:title');
        }
        elsif 
        ($scopeCode eq "service") {
        $title = $metadataElement->findvalue('./gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:citation/gmd:CI_Citation/gmd:title');
        }
        $title =~ s/[\v]//g;
        $title =~ s/^[\s]{1,}//g;
        $title =~ s/[\s]{1,}$//;
        utf8::encode($title);
#         print $title,"\n";


#     # filter title if needed
#     next unless ($title =~ /Platser/ || $title =~ /Stationsregist/ || $title =~ /Nitrat/);


	# find MD_identifier, different specifications for service or dataset
    my $dataIdentifier;
    if ($scopeCode eq "dataset") {
        $dataIdentifier = $metadataElement->findvalue('./gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:identifier/gmd:MD_Identifier');
        }
        elsif 
        ($scopeCode eq "service") {
        $dataIdentifier = $metadataElement->findvalue('./gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:citation/gmd:CI_Citation/gmd:identifier/gmd:MD_Identifier');
        }
#         print $dataIdentifier,"\n";

	# output depending on chosen scope
	if ( $scopeCode eq $scope ) {
		
		# collect metadata identifier
		push @metadataUUID, $UUID;

		# collect dataset identifier
		push @dataUUID, $dataIdentifier;


# 		# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# 		# TO EXTRACT ONLY PRIORITY DATASETS COMMENT OUT THESE LINES AND THE CLOSING BRACKETS BELOW
# 		# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #  
# 	    if ($scopeCode eq "dataset") {
# 			foreach my $resource ($metadataElement->findnodes('.//gmd:MD_Keywords')) {
# 				my @keywords = $resource->findvalue('./gmd:keyword'); 
# 				my @thesaurus = $resource->findvalue('./gmd:thesaurusName');
# 				utf8::encode(@thesaurus);
# 				utf8::encode(@keywords);
#  				if (any {$_ =~ m/(INSPIRE priority data set)/} @thesaurus) {
# 		# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #  # # # 


        print "#$count\n\n";
#         print "Scope:\n$scopeCode\n\n";
        print "Titel:\n$title\n\n";
        print "Metadata UUID:\n$UUID\n\n";
        print "Resurs UUID: \n$dataIdentifier\n\n";
       
        print REP "#$count\n\n";
#         print REP "Scope:\n$scopeCode\n\n";
        print REP "Titel:\n$title\n\n";
        print REP "Metadata UUID:\n$UUID\n\n";
        print REP "Resurs UUID: \n$dataIdentifier\n\n";

        
        # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
        # The three code sections below print links to metadata posts in three catalogs
        # and pushes to a list for http test. Change according to your catalogue link format
        
        my %mdCatalogueLinks;
        
#         # 1
#         # metadatapost CMDK, Naturvårdsverkets centrala metadatakatalogen (HR)
#         my $baselink = qq(https://metadatakatalogen.naturvardsverket.se/metadatakatalogen/GetMetaDataById?id=);
#         my $mdlink = $baselink.$UUID;
#         print "Metadata (CMDK HumanReadable):\n$mdlink\n\n";
#         print REP "Metadata (CMDK HumanReadable):\n$mdlink\n\n";
#         $mdCatalogueLinks{CMDKHR} = $mdlink;
#         
#         # 2
#         # metadatapost CMDK, Naturvårdsverkets centrala metadatakatalogen (XML)
#         my $baselinkCMDK = $catalogue.qq(request=GetRecordById&service=CSW&version=2.0.2&elementSetName=full&outputSchema=csw:IsoRecord&id=);
#         my $mdlinkCMDK = $baselinkCMDK.$UUID;
#         print "Metadata (CMDK - XML):\n$mdlinkCMDK\n\n";
#         print REP "Metadata (CMDK - XML):\n$mdlinkCMDK\n\n";
#         $mdCatalogueLinks{CMDKXML} = $mdlinkCMDK;
#         
        # 3
        # metadatapost as xml (GDSE, Nationella Geodataportalen)
        # format if Inspire
        if ($mdKwd eq "Inspire") {
        my $baselinkGDSE = qq(https://www.geodata.se/geodataportalen/srv/eng/csw-inspire?request=GetRecordById&service=CSW&version=2.0.2&elementSetName=full&outputSchema=csw:IsoRecord&id=);
        my $mdlinkGDSE = $baselinkGDSE.$UUID;
        print "Metadata (Geodata.se XML):\n$mdlinkGDSE\n\n";
        print REP "Metadata (Geodata.se XML):\n$mdlinkGDSE\n\n";
        $mdCatalogueLinks{GDSE} = $mdlinkGDSE;
		} else {
		# normal format not Inspire
        my $baselinkGDSE = qq(https://www.geodata.se/geodataportalen/srv/eng/csw?request=GetRecordById&service=CSW&version=2.0.2&elementSetName=full&outputSchema=csw:IsoRecord&id=);
        my $mdlinkGDSE = $baselinkGDSE.$UUID;
#         print "Metadata (Geodata.se XML):\n$mdlinkGDSE\n\n";
        print REP "Metadata (Geodata.se XML):\n$mdlinkGDSE\n\n";
        $mdCatalogueLinks{GDSE} = $mdlinkGDSE;
		}

		
# 		Identify which services expose a dataset 
		if ($scopeCode eq "dataset") { 
            findOperatesOn($UUID)
            }

		
# 		Parse keywords
	    if ($scopeCode eq "dataset") {
# 			# test for missing dataIdentifier
# 			if (!defined $dataIdentifier || length($dataIdentifier) < 2) {
# 				print FAIL "$title\n";
# 				print FAIL "$UUID\n";
# 				printf FAIL "Data UUID: %s ???\n\n", $dataIdentifier;
# 				}
# 	    parse keywords
        foreach my $resource ($metadataElement->findnodes('.//gmd:MD_Keywords')) {
            my @keywords = $resource->findvalue('./gmd:keyword'); 
            my @thesaurus = $resource->findvalue('./gmd:thesaurusName');
			utf8::encode(@thesaurus);
			utf8::encode(@keywords);
			if (any {$_ =~ m/(INSPIRE priority data set)/} @thesaurus) {
# 			if (any {$_ =~ m/(Publiceringsmål)/} @thesaurus)  {
# 			if (any {$_ =~ m/(GEMET)/} @thesaurus)  {
				print "\nKeyword list ... \n\n";
				print REP "\nKeyword list ... \n\n";
				print join(", ", @thesaurus),": ", join(", ", @keywords), "\n";
				print REP join(", ", @thesaurus),": ", join(", ", @keywords), "\n";
				# uncomment for lists
				$kwdTEX{$title} = join(", ", @keywords);
				}
			}
        }

        
# 		parse roles
        print "\nRoles ... \n\n";
        print REP "\nRoles ... \n\n";
		foreach my $resource ($metadataElement->findnodes('.//gmd:pointOfContact/gmd:CI_ResponsibleParty')) {
# 			print $resource,"\n";
			my $partyName = $resource->findvalue('.//gmd:organisationName'); 
			my $partyAdre = $resource->findvalue('.//gmd:electronicMailAddress');
            my $partyRole = $resource->find('.//gmd:role/gmd:CI_RoleCode/@codeListValue');
			utf8::encode($partyRole);
			utf8::encode($partyName);
			utf8::encode($partyAdre);
            print "$partyRole: $partyName ($partyAdre)\n";
            print REP "$partyRole: $partyName ($partyAdre)\n";
			}

			
# # 		Parse restrictions
#         my $constraints;
#         foreach my $resource ($metadataElement->findnodes('.//gmd:resourceConstraints')) {
#             my @constraints = $resource->findvalue('./gmd:MD_LegalConstraints'); 
# 			utf8::encode(@constraints);
# 			$constraints = join(", ", @constraints);
# 			print "\nConstraints list ... \n\n";
# 			print $constraints,"\n";
# 			print REP "\nConstraints list ... \n\n";
# 			print REP $constraints,"\n";
# 			}


# # 		Parse encoding
# 		my $characterSet;
#         foreach my $resource ($metadataElement->findnodes('.//gmd:MD_DataIdentification')) {
#             my @characterSet = $resource->findvalue('.//gmd:MD_CharacterSetCode'); 
# 			$characterSet = join(", ", @characterSet);
#             print "\ncharacterSet ... \n\n";
# 			print $characterSet,"\n";
# 			print REP "\ncharacterSet ... \n\n";
# 			print REP $characterSet,"\n";
# 			}


# # 		Parse formats
# 		my $formats;
#         foreach my $resource ($metadataElement->findnodes('.//gmd:distributionInfo')) {
#             my @formats = $resource->findvalue('.//gmd:distributorFormat');
#             $formats = join(", ", @formats);
# 			print "\nFormats ... \n\n";
# 			print $formats,"\n";
# 			print REP "\nFormats list ...\n\n";
# 			print REP $formats,"\n";
# 			}


# # 		Parse service type
# 		my $serviceType;
# 		if ($scopeCode eq "service") {
#             foreach my $resource ($metadataElement->findnodes('.//srv:serviceType')) {
#                 my @serviceType = $resource->findvalue('.//gco:LocalName'); 
#                 print "\nserviceType ... \n\n";
#                 print join(", ", @serviceType),"\n";
#                 print REP "\nserviceType ... \n\n";
#                 print REP join(", ", @serviceType),"\n";
#                 $serviceType = join(", ", @serviceType);
#                 }
#             }


# 		# test metadata links for HTTP status
# 		print "\nMetadata link test ... \n\n";
# 		print REP "\nMetadata link test ... \n\n";
# 		foreach my $link (keys %mdCatalogueLinks) {
# 			print "Test MD link $link \n";
# 			print REP "Test MD link $link \n";
# 			my $ua = LWP::UserAgent->new;
# 			$ua->timeout(9); # Time out after 5 sec wait
# 			my $req = GET "$mdCatalogueLinks{$link}";
# 			my $res = $ua->request($req);
# 			print "Status: ", $res->status_line, "\n\n";
# 			print REP "Status: ", $res->status_line, "\n\n";
# 			next if $res->status_line =~ m/(200 OK)/g;
# 			next if $res->status_line =~ m/(302 FOUND)/g;
# 			next if $res->status_line =~ m/(302 Found)/g;
# 			print FAIL "$title\n";
# 			print FAIL "$UUID\n";
# 			print FAIL "MDLINK: $link\n";
# 			print FAIL "Link: $mdCatalogueLinks{$link}\n";
# 			printf FAIL "Status: %s\n\n", $res->status_line;
# 			}
         

		my %linkTest;
# 		# parse protocols and links
# 		print "\nLink list ... \n\n";
# 		print REP "\nLink list ... \n\n";
# 		foreach my $resource ($metadataElement->findnodes('.//gmd:CI_OnlineResource')) {
# 			my @links = $resource->findvalue('.//gmd:linkage'); 
# 			my @proto = $resource->findvalue('.//gmd:protocol');
# 			for (my $e = 0; $e == scalar(@proto)-1; $e++) {
# 				printf "%10s:\n%s\n\n", $proto[$e], $links[$e];
# 				printf REP "%10s:\n%s\n\n", $proto[$e], $links[$e];
# 				$linkTest{$proto[$e]} = $links[$e];
# 				}
# 			}


        my $protocol;        
#         # test links for HTTP status
#         print "\nLink test ... \n\n";
#         print REP "\nLink test ... \n\n";
#         foreach $protocol (keys %linkTest) {
# 			# acccess only if protocol is defined (exists) and is NOT a ZIP file
#             if (defined $protocol && $linkTest{$protocol} !~ /zip/) {
#             # # uncomment to test only one type of link
# #             if ($protocol eq "HTTP:Information") {
# #             if ($protocol eq "HTTP:OGC:WMS") {
# #             if ($protocol eq "HTTP:OGC:WFS") {
# 			# funkar men krångligt att implementera
# 			# if ($linkTest{$protocol} =~ m/datavardluft/) {$linkTest{$protocol} = $linkTest{$protocol}.'request=getcapabilities'}
#             print "Test $protocol ... \n";
#             print REP "Test $protocol ... \n";
#             print $linkTest{$protocol},"\n";
#             print REP $linkTest{$protocol},"\n";
#             my $ua = LWP::UserAgent->new;
#             $ua->timeout(10); # Time out after 10 sec wait
#             my $req = GET "$linkTest{$protocol}";
#             my $res = $ua->request($req);
#             print "Status: ", $res->status_line, "\n\n";
#             print REP "Status: ", $res->status_line, "\n\n";
# 			# in case of failed status send report to fail report file
# 			next if $res->status_line =~ m/(200 OK)/g;
# 			next if $res->status_line =~ m/(302 FOUND)/g;
# 			next if $res->status_line =~ m/(302 Found)/g;
# 			print FAIL "Link failure in $scope:\n";
# 			print FAIL "$title\n";
#             print FAIL "$UUID\n";
#             print FAIL "Protocol: $protocol\n";
#             print FAIL "Link: $linkTest{$protocol}\n";
#             printf FAIL "Status: %s\n\n", $res->status_line;
# 				}
# #           }
# 		}

            
		my %adressTest;
# 		# test adresses
# 		print "\nAddress list ... \n\n";
# 		print REP "\nAdress list ... \n\n";
# 		foreach my $resource ($metadataElement->findnodes('.//gmd:CI_ResponsibleParty')) {
# 			my @email = $resource->findvalue('.//gmd:electronicMailAddress'); 
# 			my @orgnm = $resource->findvalue('./gmd:organisationName');
# 			for (my $e = 0; $e == scalar(@orgnm)-1; $e++) {
# 				utf8::encode($orgnm[$e]);
# # 				if ($email[$e] =~ 'data@naturvardsverket.se')  {
# 					printf "%10s:\n%s\n\n", $orgnm[$e], $email[$e];
# 					printf REP "%10s:\n%s\n\n", $orgnm[$e], $email[$e];
# # 					}
# 				$adressTest{$orgnm[$e]} = $email[$e];
# 				}
# 			}


		# for services: parse services linked resources
		if ($scopeCode eq "service") {
			
			print "Linked resources:\n";
			print REP "Linked resources:\n";

			my $id;
			foreach my $resource ($metadataElement->findnodes('./gmd:identificationInfo/srv:SV_ServiceIdentification/srv:operatesOn')) {
				$id = $resource->findvalue('@uuidref');
				if (!defined $id || $id lt 1) {
	 				$id = $resource->findvalue('@xlink:href');
					}
				print $id,"\n";
				print REP $id,"\n";
				}
			}


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# 	# uncomment for writing to external lists
# 	$midTEX{$title} = $UUID;
# 	$didTEX{$title} = $dataIdentifier;
# 	$lnkTEX{$title} = $mdlink;


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # 	uncomment to report an auxiliary file
# 	{
# # 	if ( $linkTest{'HTTP:OGC:WFS'} =~ /geodatatest/ || $linkTest{'HTTP:OGC:WMS'} =~ /geodatatest/) {
# # 	if ( $linkTest{'HTTP:OGC:WMS'} =~ /geodatatest/ ) {
# #     if ( defined $kwdTEX{$title} ) {
# # 	if ($title =~ /Platser/ || $title =~ /Stationsregist/ || $title =~ /Nitrat/) {
# # 	if ($title =~ /Produktion/ || $title =~ /Inspire/) {
# # 	if ($linkTest{'HTTP:Nedladdning:Atom'} =~ /gml/) {
# #     if ($formats =~ /Shape/ || $formats =~ /SHP/) {
# # 	my @list = qw(94c363b1-68aa-42c0-a25d-35e236973afd 12dacd90-1479-4a39-be35-087604ce2b11 4c15a916-f192-4ae6-ac2c-478b966771fa 84ec6102-56e4-40c3-9633-ffe54871fb93 575bf956-9752-4fc0-a079-54c3df17d7e4);
# # 	if (any {$_ eq $UUID} @list) {
# 
# # 		print AUX $countx,"\n";
# 
# # 		print AUX $title,"\n";
# # 		print AUX $UUID,"\n";
# # # 		print AUX $mdlink,"\n";
# # 		print AUX $mdCatalogueLinks{GDSE},"\n";
# 
# 		print AUX "$title, $UUID, $dataIdentifier\n";
# 
# # 		print AUX $formats,"\n";
# #         foreach $protocol (keys %linkTest) {
# #             if (defined $protocol && $linkTest{$protocol} =~ /zip/) {
# #                 print AUX $title,"\n";
# #                 print AUX $linkTest{$protocol},"\n";
# #                 print AUX "\n";
# #                 }
# #             }
# # 
# # 		print AUX $dataIdentifier,"\n";
# #       print AUX $kwdTEX{$title},"\n";
# # 		print AUX "ATOM: $linkTest{'HTTP:Nedladdning:Atom'} \n";
# # 		print AUX "WMS: $linkTest{'HTTP:OGC:WMS'} \n";
# # 		print AUX "WFS: $linkTest{'HTTP:OGC:WFS'} \n";
# # 		print AUX "\n";
# # 		
# 		$countx++
# # 		}
# 	}


               
    print "----------------------------------------------------------------\n\n";
    print REP "----------------------------------------------------------------\n\n";
    $count++;


# 		# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
#       # THESE CLOSING BRACKETS SHOULD BE COMMENTED OUT WHEN NOT EXTRACTING PRIORITY METADATA
#    			}
# 		}
# 	}    
# 		# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    
        }
    }


close AUX;
close FAIL;
close REP;

# delete files if size == 0
if (-s "metadata_$date-$scope.aux" == 0) { unlink "metadata_$date-$scope.aux" }
if (-s "metadata_$date-$scope.fail" == 0) { unlink "metadata_$date-$scope.fail" }


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # print NORMAL list
# open (TXT, '>', 'list.txt');
# # sort
# foreach my $key (sort keys %listTEX) {
# 	print TXT "$key\n$listTEX{$key}\n\n";
# 	}
# 
# close TXT;


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # print TEX list
# open (TEX, '>', "list_$scope.tex");
# # sort
# my $ccountz = 1;
# foreach my $key (sort keys %midTEX) {
# 	# print TEX 
# 	my $mid = $midTEX{$key};
# 	my $did = $didTEX{$key};
# 	my $lnk = $lnkTEX{$key};
# 
# my $lopd = qq();
# if (defined $kwdTEX{$key}) {
# 	$lopd = qq({\\color{red}LOPD});
# 	}
# 
# # utf8::encode($key);
# 	
# print TEX <<EOF;
# ($ccountz) $key $lopd \\\\
# \\href{$lnk}{Metadata} \\\\
# \\\\
# EOF
# 
# $ccountz++
# 
# }
# close TEX;


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # print NORMAL list
# open (TXT, '>', 'list.txt');
# # sort
# foreach my $key (sort keys %listTEX) {
# 	my $link = $listTEX{$key};
# 	my $kywd = $kywdTEX{$key};
# 	my $uuid = $uuidTEX{$key};
# print TXT <<EOF;
# $key
# UUID: $uuid
# LOPD: $kywd
# Geodataportalen: Metadata (Ja), Visning (Ja), Nedladdning (Ja)
# Inspire Geoportal: Metadata (Ja), Visning (Ja), Nedladdning (Nej)
# Harmonisering: 2020
# \n
# EOF
# }
# close TXT;


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

# # check for non unique metadata UUID
# # print join("\n", @metadataUUID);
# my @metadataUUID_uniq = uniq @metadataUUID;
# 
# while (@metadataUUID > 0) {
# 	my $value = pop @metadataUUID;
# 	foreach my $aver(@metadataUUID) {
# 		if ($aver eq $value) { print "$aver = $value\n"}
# 		}
# 	}

# # check for non unique UUID
# # print join("\n", @dataUUID);
# my @dataUUID_uniq = uniq @dataUUID;
# 
# while (@dataUUID > 0) {
# 	my $value = pop @dataUUID;
# 	foreach my $aver(@dataUUID) {
# 		if ($aver eq $value) { print "$aver = $value\n"}
# 		}
# 	}



# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# this subroutine finds which datasets a given service operates on
#

sub findOperatesOn {

	my $UUID_main = shift @_;

	# define DOM
	my $domsub = XML::LibXML->load_xml( location => $metadataFile, no_blanks => 1  );
	my $xpcsub = XML::LibXML::XPathContext->new($domsub);
	$xpcsub->registerNs('csw', 'http://www.opengis.net/cat/csw/2.0.2');
	$xpcsub->registerNs('xsi', 'http://www.w3.org/2001/XMLSchema-instance');
	$xpcsub->registerNs('gmd', 'http://www.isotc211.org/2005/gmd');
	$xpcsub->registerNs('srv', 'http://www.isotc211.org/2005/srv');
	$xpcsub->registerNs('gco', 'http://www.isotc211.org/2005/gco');
	$xpcsub->registerNs('xlink', 'http://www.w3.org/1999/xlink');
	$xpcsub->registerNs('gts', 'http://www.isotc211.org/2005/gts');
	$xpcsub->registerNs('gml', 'http://www.opengis.net/gml');
	$xpcsub->registerNs('geonet', 'http://www.fao.org/geonetwork');

	# start loop, iterate over each MD_Metadata element and its children
	foreach my $metadataElement ($xpcsub->findnodes('//gmd:MD_Metadata')) {
	
		# find scope: dataset or service
		my $scopeCode_sub = $metadataElement->findvalue('./gmd:dataQualityInfo/gmd:DQ_DataQuality/gmd:scope/gmd:DQ_Scope/gmd:level/gmd:MD_ScopeCode/@codeListValue');

		# skip datasets
		next if ($scopeCode_sub eq 'dataset');

		# find UUID
		my $UUIDsub  = $metadataElement->findvalue('./gmd:fileIdentifier');
		$UUIDsub =~ s/[\v\h\s]//g;

		# find title
		my $title_sub;
		$title_sub = $metadataElement->findvalue('./gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:citation/gmd:CI_Citation/gmd:title');
		$title_sub =~ s/[\v]//g;
		$title_sub =~ s/^[\s]{1,}//g;
		$title_sub =~ s/[\s]{1,}$//;
		utf8::encode($title_sub);

		# find operatesOn
		my $operatesOn = $metadataElement->findvalue('./gmd:identificationInfo/srv:SV_ServiceIdentification/srv:operatesOn/@xlink:href');
		
		if ($operatesOn =~ m/$UUID_main/) {
		
			my $baselinkGDSE = qq(https://www.geodata.se/geodataportalen/srv/eng/csw-inspire?request=GetRecordById&service=CSW&version=2.0.2&elementSetName=full&outputSchema=csw:IsoRecord&id=);
			my $mdlinkGDSE = $baselinkGDSE.$UUIDsub;
			
			print "Linked resource operatesOn:\n";
			print $title_sub, "\n";
			print $UUIDsub, "\n";
			print $mdlinkGDSE, "\n";
			print "\n";

			print REP "Linked resource operatesOn:\n";
			print REP $title_sub, "\n";
			print REP $UUIDsub, "\n";
			print REP $mdlinkGDSE, "\n";
			print REP "\n";

			}
			
		}
	}
