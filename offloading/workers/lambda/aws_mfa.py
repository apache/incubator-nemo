#!/usr/bin/python3
import sys
import configparser
import json
import os
from os.path import expanduser

if(len(sys.argv) <= 1 ):
    requestedProfile = 'default'
    # exit("Need named profile")
else:
    requestedProfile = sys.argv[1]

home = expanduser("~")
awsConfig = configparser.ConfigParser()
awsCred   = configparser.ConfigParser()
awsOrigCred   = configparser.ConfigParser()

# Initialize
awsOrigCred.read('%s/.aws/wonook_accessKeys' % home)
awsCred.read('%s/.aws/credentials' % home)
awsCred[requestedProfile]['aws_access_key_id']     = awsOrigCred[requestedProfile]['aws_access_key_id'] 
awsCred[requestedProfile]['aws_secret_access_key'] = awsOrigCred[requestedProfile]['aws_secret_access_key']
del awsCred[requestedProfile]['aws_session_token']
with open('%s/.aws/credentials' % home, 'w') as awsCredfile:
    awsCred.write(awsCredfile)

# Read configs
awsConfig.read("%s/.aws/config" % home)
awsCred.read('%s/.aws/credentials' % home)

# Read mfa_arn
try:
    mfaARN = awsConfig[requestedProfile]['mfa_arn']
except KeyError:
    try:
        mfaARN = awsConfig['default']['mfa_arn']
    except KeyError:
        exit("Need MFA serial in config file")

profiles = set( awsCred.sections())
configprofiles = set( awsConfig.sections())

if( requestedProfile in profiles and requestedProfile in configprofiles):
    print("Updating %s profile" % requestedProfile)
else:
    if( "profile " + requestedProfile in configprofiles):
        print("Creating %s credentials profile" % requestedProfile)
        awsCred.add_section(requestedProfile)
    else:
        exit("No such profile \"%s\" in config" % requestedProfile )

# Get OTP
try:
    OneTimeNumber = int(input("OTP from device: "))
except ValueError:
    exit("OTP must be a number")

# Main request
response = os.popen("aws --profile %s sts get-session-token --serial-number  %s --token-code %s" % (requestedProfile,
                                                                                                 mfaARN,
                                                                                                 str(OneTimeNumber).zfill(6))).read()

try:
    myjson = json.loads(response)
except json.decoder.JSONDecodeError:
    print(response)
    exit("AWS was not happy with that one")

# Overwrite
awsCred[requestedProfile]['aws_access_key_id']     = myjson['Credentials']['AccessKeyId']
awsCred[requestedProfile]['aws_secret_access_key'] = myjson['Credentials']['SecretAccessKey']
awsCred[requestedProfile]['aws_session_token']     = myjson['Credentials']['SessionToken']

with open('%s/.aws/credentials' % home, 'w') as awsCredfile:
    awsCred.write(awsCredfile)
