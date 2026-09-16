package acl

import "fmt"

// AuthorityTaskLifetimeKey binds a grant and every descendant to one explicitly
// authorized background task, regardless of the recipient's audience type.
const AuthorityTaskLifetimeKey = "authority_task_lifetime"

func InheritTaskLifetime(metadata map[string]interface{}, parent *AuthorityGrant) map[string]interface{} {
	result := make(map[string]interface{}, len(metadata)+1)
	for key, value := range metadata {
		result[key] = value
	}
	if value, exists := parent.Metadata[AuthorityTaskLifetimeKey]; exists {
		result[AuthorityTaskLifetimeKey] = value
	}
	return result
}

// ValidateTaskLifetime fails closed when live task state is unavailable.
func ValidateTaskLifetime(grant *AuthorityGrant, audience GrantAudienceContext) error {
	value, exists := grant.Metadata[AuthorityTaskLifetimeKey]
	if !exists {
		return nil
	}
	taskID, ok := value.(string)
	if !ok || taskID == "" || audience.TaskActive == nil || !audience.TaskActive(taskID) {
		return fmt.Errorf("%w: authorized task is no longer active", ErrAuthorityGrantAudienceMismatch)
	}
	return nil
}
