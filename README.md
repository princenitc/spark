### Minikube setup 

- `minikube profile list` - list all the profile of cluster available on minikube. 
- **Profiles** - In Minikube the concept of profile refers to different configurations of the clusters that can be created on a single machine. 
- To Switch to a Particular profile : `minikube profile <profile_name>`
- To see the context and details of a minikube cluster : `kubectl config get-contexts
  `
- To use a minikube context: `kubectl config use-context <context-name>
  `
- `minikube stop` : Stops the default minikube profile.
- `minikube stop --profile <profile-name>` OR ``minikube stop -p <profile-name>``: Stops a specific minikube profile.
- `minikube delete` : Deletes current active profile 
- `minikube delete --profile <profile-name>` OR `minikube delete -p <profile-name>` : Deletes a Specific profile
- 