import subprocess,json

#Script that creates a new version of each function that has been modified after a deploy
#and redirects the alias towards this new version
def main():
    functions=json.loads(str(subprocess.check_output("aws lambda list-functions"), "utf-8"))
    #Iterates through all lambda functions to find their names, version and aliases
    for x in functions["Functions"]:
        name=x["FunctionName"]
        alias=json.loads(str(subprocess.check_output(f'aws lambda list-aliases --function-name {name}'), "utf-8"))["Aliases"]
        
        #Assuming we only have one alias per function and some don't (like serverless-api)
        if len(alias)>0:
            current=alias[0]["FunctionVersion"]
            #Creates a new version if it detects changes between $LATEST and the last version that was made(Should be the version of the alias)
            #Returns the same version if no changes have been detected
            new=json.loads(str(subprocess.check_output(f'aws lambda publish-version --function-name {name}'), "latin-1"))["Version"]
            
            #Update alias version if it differs from the new one created
            if current!=new and name.find("-STMFetchGTFSVehiclePositions-")==-1:
                subprocess.run(f'aws lambda update-alias --function-name {name} --name {alias[0]["Name"]} --function-version {new}')
                print(f'{name} alias has been redirected to the latest version available!')
            else:
                print(f'{name} is already up to date!')
                
main()