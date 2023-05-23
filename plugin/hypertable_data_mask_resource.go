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
type hypertableDataMaskResource struct {
	Client *api.Client
}

type hypertableDataMaskResourceModel struct {
	PolicyId      string `pctsdk:"policy_id"`
	HypertableId  string `pctsdk:"hypertable_id"`
	UserEmail     string `pctsdk:"user_email"`
	GroupName     string `pctsdk:"group_name"`
	MaskingOption string `pctsdk:"masking_option"`
	Column        string `pctsdk:"column"`
}

// Ensure the implementation satisfies the expected interfaces.
var (
	_ schema.ResourceService = &hypertableDataMaskResource{}
)

// Helper function to return a resource service instance.
func NewHypertableDataMaskResource() schema.ResourceService {
	return &hypertableDataMaskResource{}
}

// Metadata returns the resource type name.
// It is always provider name + "_" + resource type name.
func (r *hypertableDataMaskResource) Metadata(req *schema.ServiceRequest) *schema.ServiceResponse {
	return &schema.ServiceResponse{
		TypeName: req.TypeName + "_hypertable_data_mask",
	}
}

// Configure adds the provider configured client to the resource.
func (r *hypertableDataMaskResource) Configure(req *schema.ServiceRequest) *schema.ServiceResponse {
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
func (r *hypertableDataMaskResource) Schema() *schema.ServiceResponse {
	s := &schema.Schema{
		Description: "Hypertable data mask resource for Zipstack Cloud",
		Attributes: map[string]schema.Attribute{
			"policy_id": &schema.StringAttribute{
				Description: "Policy ID",
				Computed:    true,
			},
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
			"masking_option": &schema.StringAttribute{
				Description: "Masking Option",
				Required:    true,
			},
			"column": &schema.StringAttribute{
				Description: "Column",
				Required:    true,
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
func (r *hypertableDataMaskResource) Create(req *schema.ServiceRequest) *schema.ServiceResponse {
	// logger := fwhelpers.GetLogger()

	// Retrieve values from plan
	var plan hypertableDataMaskResourceModel
	err := fwhelpers.UnpackModel(req.PlanContents, &plan)
	if err != nil {
		return schema.ErrorResponse(err)
	}

	if plan.UserEmail != "" && plan.GroupName != "" {
		return schema.ErrorResponse(fmt.Errorf(
			"both user email and group name cannot be provided",
		))
	}
	if plan.Column == "" {
		return schema.ErrorResponse(fmt.Errorf(
			"column is required",
		))
	}

	// Generate API request body from plan
	body := api.HypertableDataMask{}
	body.HypertableId = plan.HypertableId
	body.UserEmail = plan.UserEmail
	body.GroupName = plan.GroupName
	body.MaskingOption = plan.MaskingOption
	body.Column = plan.Column

	// Create or update hypertable data mask
	status, err := r.Client.CreateHypertableDataMask(body)
	if err != nil {
		return schema.ErrorResponse(err)
	}
	if status != "true" {
		return schema.ErrorResponse(fmt.Errorf(
			"failed to create data mask",
		))
	}

	// Update state with refreshed value
	state := hypertableDataMaskResourceModel{}

	// Query using created state.
	htDataMasks, err := r.Client.ReadHypertableDataMask(plan.HypertableId)
	if err != nil {
		return schema.ErrorResponse(err)
	}

	// For a given hypertable, the list of users or groups
	// is returned. Hence, we need to retrieve the matching
	// hypertable ID and user or group combination.
	found := false
	if len(plan.UserEmail) > 0 {
		for _, policy := range htDataMasks.Users {
			if policy.Member == plan.UserEmail && policy.Column == plan.Column {
				state.PolicyId = policy.PolicyId
				state.UserEmail = policy.Member
				state.MaskingOption = policy.MaskingOption
				state.Column = policy.Column

				found = true
				break
			}
		}
	}
	if !found && len(plan.GroupName) > 0 {
		for _, policy := range htDataMasks.Groups {
			if policy.Member == plan.GroupName && policy.Column == plan.Column {
				state.PolicyId = policy.PolicyId
				state.GroupName = policy.Member
				state.MaskingOption = policy.MaskingOption
				state.Column = policy.Column

				found = true
				break
			}
		}
	}

	if !found {
		return schema.ErrorResponse(fmt.Errorf("failed to create data mask"))
	}

	state.HypertableId = htDataMasks.HypertableId

	// Set refreshed state
	userOrGroup := plan.UserEmail
	if len(userOrGroup) == 0 {
		userOrGroup = plan.GroupName
	}
	// We create a resource for each user or group, but
	// the retrieval from provider is via hypertable ID.
	// Hence state ID needs to be a combination of both.
	stateId := r.Client.GetHypertableDataMaskStateId(
		state.HypertableId, userOrGroup, state.Column,
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
func (r *hypertableDataMaskResource) Read(req *schema.ServiceRequest) *schema.ServiceResponse {
	// logger := fwhelpers.GetLogger()

	var state hypertableDataMaskResourceModel

	// Get current state
	err := fwhelpers.UnpackModel(req.StateContents, &state)
	if err != nil {
		return schema.ErrorResponse(err)
	}

	res := schema.ServiceResponse{}

	if req.StateID != "" {
		hypertableId, userOrGroup, column := "", "", ""
		parts := r.Client.ParseHypertableDataMaskStateId(
			req.StateID,
		)
		if len(parts) == 3 {
			hypertableId, userOrGroup, column = parts[0], parts[1], parts[2]
		}

		if hypertableId == "" || userOrGroup == "" {
			// No previous state exists.
			res.StateID = ""
			res.StateLastUpdated = ""
		} else {
			// Query using existing previous state.
			htDataMasks, err := r.Client.ReadHypertableDataMask(hypertableId)

			if err != nil && err.Error() == "404 Not Found" {
				// No previous state exists.
				res.StateID = ""
				res.StateLastUpdated = ""
			} else if err != nil {
				return schema.ErrorResponse(err)
			} else {
				// Update state with refreshed value
				state.PolicyId = ""
				state.HypertableId = ""
				state.UserEmail = ""
				state.GroupName = ""
				state.MaskingOption = ""
				state.Column = ""

				// For a given hypertable, the list of users or groups
				// is returned. Hence, we need to retrieve the matching
				// hypertable ID and user or group combination.
				found := false
				for _, policy := range htDataMasks.Users {
					if policy.Member == userOrGroup && policy.Column == column {
						state.PolicyId = policy.PolicyId
						state.UserEmail = policy.Member
						state.MaskingOption = policy.MaskingOption
						state.Column = policy.Column

						found = true
						break
					}
				}
				if !found {
					for _, policy := range htDataMasks.Groups {
						if policy.Member == userOrGroup && policy.Column == column {
							state.PolicyId = policy.PolicyId
							state.GroupName = policy.Member
							state.MaskingOption = policy.MaskingOption
							state.Column = policy.Column

							found = true
							break
						}
					}
				}

				if found {
					state.HypertableId = htDataMasks.HypertableId

					res.StateID = r.Client.GetHypertableDataMaskStateId(
						hypertableId, userOrGroup, column,
					)
					res.StateLastUpdated = time.Now().UTC().Format(time.RFC850)
				} else {
					// No previous state exists.
					res.StateID = ""
					res.StateLastUpdated = ""
				}
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
func (r *hypertableDataMaskResource) Update(req *schema.ServiceRequest) *schema.ServiceResponse {
	// logger := fwhelpers.GetLogger()

	return schema.ErrorResponse(fmt.Errorf(
		"update is not supported",
	))
}

// Delete deletes the resource and removes the state on success.
func (r *hypertableDataMaskResource) Delete(req *schema.ServiceRequest) *schema.ServiceResponse {
	// logger := fwhelpers.GetLogger()

	hypertableId, userOrGroup, column := "", "", ""
	parts := r.Client.ParseHypertableDataMaskStateId(
		req.StateID,
	)
	if len(parts) == 3 {
		hypertableId, userOrGroup, column = parts[0], parts[1], parts[2]
	} else {
		return schema.ErrorResponse(fmt.Errorf(
			"invalid hypertable ID or user or group",
		))
	}

	// Delete existing source
	body := api.HypertableDataMask{}
	body.HypertableId = hypertableId
	if strings.Contains(userOrGroup, "@") {
		body.UserEmail = userOrGroup
	} else {
		body.GroupName = userOrGroup
	}
	body.Column = column

	err := r.Client.DeleteHypertableDataMask(body)
	if err != nil {
		return schema.ErrorResponse(err)
	}

	return &schema.ServiceResponse{}
}
