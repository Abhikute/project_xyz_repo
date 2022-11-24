import sys
def url_checker(url):
	try:
		#Get Url
		get = requests.get(url)
		# if the request succeeds 
		if get.status_code == 200:
			return(f"{url}: is reachable")
		else:
			return(f"{url}: is Not reachable, status_code: {get.status_code}")

	#Exception
	except requests.exceptions.RequestException as e:
        # print URL with Errs
		raise SystemExit(f"{url}: is Not reachable \nErr: {e}")
		
if __name__ == '__main__':
    # Map command line arguments to function arguments.
    url_checker(*sys.argv[1:])
