package plugin

import (
	"fmt"
	"strings"
	"time"

	"github.com/zipstack/pct-plugin-framework/fwhelpers"
	"github.com/zipstack/pct-plugin-framework/schema"

	"github.com/zipstack/pct-provider-zipstack-cloud/api"
)

// Resource implementation.
type hypertableAccessControlResource struct {
	Client *api.Client
}

type hypertableAccessControlResourceModel struct {
	HypertableId string `pctsdk:"hypertable_id"`
	UserEmail    string `pctsdk:"user_email"`
	GroupName    string `pctsdk:"group_name"`
}

// Ensure the implementation satisfies the expected interfaces.
var (
	_ schema.ResourceService = &hypertableAccessControlResource{}
)

// Helper function to return a resource service instance.
func NewHypertableAccessControlResource() schema.ResourceService {
	return &hypertableAccessControlResource{}
}

// Metadata returns the resource type name.
// It is always provider name + "_" + resource type name.
func (r *hypertableAccessControlResource) Metadata(req *schema.ServiceRequest) *schema.ServiceResponse {
	return &schema.ServiceResponse{
		TypeName: req.TypeName + "_hypertable_access_control",
	}
}

// Configure adds the provider configured client to the resource.
func (r *hypertableAccessControlResource) Configure(req *schema.ServiceRequest) *schema.ServiceResponse {
	if req.ResourceData == "" {
		return schema.ErrorResponse(fmt.Errorf("no data provided to configure resource"))
	}

	var creds map[string]string
	err := fwhelpers.Decode(req.ResourceData, &creds)
	if err != nil {
		return schema.ErrorResponse(err)
	}

	client, err := api.NewClient(
		creds["host"], creds["organisationname"],
		creds["email"], creds["password"],
	)
	if err != nil {
		return schema.ErrorResponse(fmt.Errorf("malformed data provided to configure resource"))
	}

	r.Client = client

	return &schema.ServiceResponse{}
}

// Schema defines the schema for the resource.
func (r *hypertableAccessControlResource) Schema() *schema.ServiceResponse {
	s := &schema.Schema{
		Description: "Hypertable access control resource for Zipstack Cloud",
		Attributes: map[string]schema.Attribute{
			"hypertable_id": &schema.StringAttribute{
				Description: "Hypertable ID",
				Required:    true,
			},
			"user_email": &schema.StringAttribute{
				Description: "User Email",
				Required:    true,
				Optional:    true,
			},
			"group_name": &schema.StringAttribute{
				Description: "Group Name",
				Required:    true,
				Optional:    true,
			},
		},
	}

	sEnc, err := fwhelpers.Encode(s)
	if err != nil {
		return schema.ErrorResponse(err)
	}

	return &schema.ServiceResponse{
		SchemaContents: sEnc,
	}
}

// Create a new resource
func (r *hypertableAccessControlResource) Create(req *schema.ServiceRequest) *schema.ServiceResponse {
	// logger := fwhelpers.GetLogger()

	// Retrieve values from plan
	var plan hypertableAccessControlResourceModel
	err := fwhelpers.UnpackModel(req.PlanContents, &plan)
	if err != nil {
		return schema.ErrorResponse(err)
	}

	if plan.UserEmail != "" && plan.GroupName != "" {
		return schema.ErrorResponse(fmt.Errorf(
			"both user email and group name cannot be provided",
		))
	}

	// Generate API request body from plan
	body := api.HypertableAccessControl{}
	body.HypertableId = plan.HypertableId
	body.UserEmail = plan.UserEmail
	body.GroupName = plan.GroupName

	// Create or update hypertable access control
	status, err := r.Client.CreateHypertableAccessControl(body)
	if err != nil {
		return schema.ErrorResponse(err)
	}
	if status != "true" {
		return schema.ErrorResponse(fmt.Errorf(
			"failed to update access control",
		))
	}

	// Update resource state with response body
	state := hypertableAccessControlResourceModel{}
	state.HypertableId = plan.HypertableId
	state.UserEmail = plan.UserEmail
	state.GroupName = plan.GroupName

	// Set refreshed state
	userOrGroup := plan.UserEmail
	if len(userOrGroup) == 0 {
		userOrGroup = plan.GroupName
	}
	// We create a resource for each user or group, but
	// the retrieval from provider is via hypertable ID.
	// Hence state ID needs to be a combination of both.
	stateId := r.Client.GetHypertableAccessControlStateId(
		plan.HypertableId, userOrGroup,
	)
	stateEnc, err := fwhelpers.PackModel(nil, &state)
	if err != nil {
		return schema.ErrorResponse(err)
	}

	return &schema.ServiceResponse{
		StateID:          stateId,
		StateContents:    stateEnc,
		StateLastUpdated: time.Now().Format(time.RFC850),
	}
}

// Read resource information
func (r *hypertableAccessControlResource) Read(req *schema.ServiceRequest) *schema.ServiceResponse {
	// logger := fwhelpers.GetLogger()

	var state hypertableAccessControlResourceModel

	// Get current state
	err := fwhelpers.UnpackModel(req.StateContents, &state)
	if err != nil {
		return schema.ErrorResponse(err)
	}

	res := schema.ServiceResponse{}

	if req.StateID != "" {
		hypertableId, userOrGroup := "", ""
		parts := r.Client.ParseHypertableAccessControlStateId(
			req.StateID,
		)
		if len(parts) == 2 {
			hypertableId, userOrGroup = parts[0], parts[1]
		}

		if hypertableId == "" || userOrGroup == "" {
			// No previous state exists.
			res.StateID = ""
			res.StateLastUpdated = ""
		} else {
			// Query using existing previous state.
			htACL, err := r.Client.ReadHypertableAccessControl(hypertableId)
			if err != nil {
				return schema.ErrorResponse(err)
			}

			// Update state with refreshed value
			state.HypertableId = ""
			state.UserEmail = ""
			state.GroupName = ""

			// For a given hypertable, the list of users or groups
			// is returned. Hence, we need to retrieve the matching
			// hypertable ID and user or group combination.
			found := false
			for _, policy := range htACL.Users {
				if policy.Member == userOrGroup {
					state.UserEmail = policy.Member
					found = true
					break
				}
			}
			if !found {
				for _, policy := range htACL.Groups {
					if policy.Member == userOrGroup {
						state.GroupName = policy.Member
						found = true
						break
					}
				}
			}

			if found {
				state.HypertableId = htACL.HypertableId

				res.StateID = r.Client.GetHypertableAccessControlStateId(
					hypertableId, userOrGroup,
				)
				res.StateLastUpdated = time.Now().UTC().Format(time.RFC850)
			} else {
				// No previous state exists.
				res.StateID = ""
				res.StateLastUpdated = ""
			}
		}
	} else {
		// No previous state exists.
		res.StateID = ""
		res.StateLastUpdated = ""
	}

	// Set refreshed state
	stateEnc, err := fwhelpers.PackModel(nil, &state)
	if err != nil {
		return schema.ErrorResponse(err)
	}
	res.StateContents = stateEnc

	return &res
}

// Update the resource information
func (r *hypertableAccessControlResource) Update(req *schema.ServiceRequest) *schema.ServiceResponse {
	// logger := fwhelpers.GetLogger()

	return schema.ErrorResponse(fmt.Errorf(
		"update is not supported",
	))
}

// Delete deletes the resource and removes the state on success.
func (r *hypertableAccessControlResource) Delete(req *schema.ServiceRequest) *schema.ServiceResponse {
	// logger := fwhelpers.GetLogger()

	hypertableId, userOrGroup := "", ""
	parts := r.Client.ParseHypertableAccessControlStateId(
		req.StateID,
	)
	if len(parts) == 2 {
		hypertableId, userOrGroup = parts[0], parts[1]
	} else {
		return schema.ErrorResponse(fmt.Errorf(
			"invalid hypertable ID or user or group",
		))
	}

	// Delete existing source
	body := api.HypertableAccessControl{}
	body.HypertableId = hypertableId
	if strings.Contains(userOrGroup, "@") {
		body.UserEmail = userOrGroup
	} else {
		body.GroupName = userOrGroup
	}

	err := r.Client.DeleteHypertableAccessControl(body)
	if err != nil {
		return schema.ErrorResponse(err)
	}

	return &schema.ServiceResponse{}
}
