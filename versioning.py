from subprocess import check_output,run, CalledProcessError
from json import loads, dump

#Script that creates a new version of each function that has been modified after a deploy
#and redirects the alias towards this new version
def main():
    functions_dict={}
    functions=loads(str(check_output('aws lambda list-functions --query "sort_by(Functions, &FunctionName)"'), "utf-8"))
    #Iterates through all lambda functions to find their names, version and aliases
    for x in functions:
        name=x["FunctionName"]
        alias=loads(str(check_output(f'aws lambda list-aliases --function-name {name}'), "latin-1"))["Aliases"]
        
        #Assuming we only have one alias per function and some don't (like serverless-api)
        if len(alias)>0:
            new=x["CodeSha256"]
            current=loads(str(check_output(f'aws lambda get-function --function-name {alias[0]["AliasArn"]}'), "latin-1"))["Configuration"]["CodeSha256"]
            #AWS uses the Sha256 code to check if two versions differ so we do the same here
            if(new==current):
                functions_dict[name.split("-")[2]]=0
            else:
                try:
                    run(f'aws lambda delete-alias --name {alias} --function-name {name}')
                    functions_dict[name.split("-")[2]]=1
                except CalledProcessError as e:
                    functions_dict[name.split("-")[2]]=0
                    #Save error to log
                    with open('delete_alias_log.txt', 'a') as log:
                        log.write(f'There was an error with deleting the alias of {name}.\n{e}\n\n')
                
    with open("functions.json", "w") as outfile:
        dump(functions_dict, outfile)
        