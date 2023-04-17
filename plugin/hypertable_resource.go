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
type hypertableResource struct {
	Client *api.Client
}

type hypertableResourceModel struct {
	Id          string   `pctsdk:"id"`
	Name        string   `pctsdk:"name"`
	Description string   `pctsdk:"description"`
	ShortName   string   `pctsdk:"short_name"`
	Tags        []string `pctsdk:"tags"`
	Admins      []string `pctsdk:"admins"`
	RefreshMode string   `pctsdk:"refresh_mode"`
	SqlSelect   string   `pctsdk:"sql_select"`
}

// Ensure the implementation satisfies the expected interfaces.
var (
	_ schema.ResourceService = &hypertableResource{}
)

// Helper function to return a resource service instance.
func NewHypertableResource() schema.ResourceService {
	return &hypertableResource{}
}

// Metadata returns the resource type name.
// It is always provider name + "_" + resource type name.
func (r *hypertableResource) Metadata(req *schema.ServiceRequest) *schema.ServiceResponse {
	return &schema.ServiceResponse{
		TypeName: req.TypeName + "_hypertable",
	}
}

// Configure adds the provider configured client to the resource.
func (r *hypertableResource) Configure(req *schema.ServiceRequest) *schema.ServiceResponse {
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
func (r *hypertableResource) Schema() *schema.ServiceResponse {
	s := &schema.Schema{
		Description: "Hypertable resource for ZMesh",
		Attributes: map[string]schema.Attribute{
			"id": &schema.StringAttribute{
				Description: "ID",
				Computed:    true,
			},
			"name": &schema.StringAttribute{
				Description: "Name",
				Required:    true,
			},
			"short_name": &schema.StringAttribute{
				Description: "Short Name",
				Required:    true,
			},
			"description": &schema.StringAttribute{
				Description: "Description",
				Required:    true,
				Optional:    true,
			},
			"tags": &schema.ListAttribute{
				Description: "Tags",
				Required:    true,
				Optional:    true,
				NestedAttribute: &schema.StringAttribute{
					Description: "Tag",
					Required:    true,
					Optional:    true,
				},
			},
			"admins": &schema.ListAttribute{
				Description: "Admins",
				Required:    false,
				NestedAttribute: &schema.StringAttribute{
					Description: "Admin",
					Required:    true,
					Optional:    true,
				},
			},
			"refresh_mode": &schema.StringAttribute{
				Description: "Refresh Mode",
				Required:    true,
			},
			"sql_select": &schema.StringAttribute{
				Description: "SQL Select",
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
func (r *hypertableResource) Create(req *schema.ServiceRequest) *schema.ServiceResponse {
	// logger := fwhelpers.GetLogger()

	// Retrieve values from plan
	var plan hypertableResourceModel
	err := fwhelpers.UnpackModel(req.PlanContents, &plan)
	if err != nil {
		return schema.ErrorResponse(err)
	}

	// Generate API request body from plan
	body := api.Hypertable{}
	body.Name = plan.Name
	body.Description = plan.Description
	body.ShortName = plan.ShortName
	body.Tags = plan.Tags
	body.Admins = plan.Admins
	body.RefreshMode = plan.RefreshMode
	body.SqlSelect = plan.SqlSelect

	// Create new source
	hypertable, err := r.Client.CreateHypertable(body)
	if err != nil {
		return schema.ErrorResponse(err)
	}

	// Update resource state with response body
	state := hypertableResourceModel{}
	state.Id = hypertable.Id
	state.Name = plan.Name
	state.Description = plan.Description
	state.ShortName = plan.ShortName
	state.Tags = plan.Tags
	state.Admins = plan.Admins
	state.RefreshMode = plan.RefreshMode
	state.SqlSelect = plan.SqlSelect

	// Set refreshed state
	stateEnc, err := fwhelpers.PackModel(nil, &state)
	if err != nil {
		return schema.ErrorResponse(err)
	}

	return &schema.ServiceResponse{
		StateID:          hypertable.Id,
		StateContents:    stateEnc,
		StateLastUpdated: time.Now().Format(time.RFC850),
	}
}

// Read resource information
func (r *hypertableResource) Read(req *schema.ServiceRequest) *schema.ServiceResponse {
	// logger := fwhelpers.GetLogger()

	var state hypertableResourceModel

	// Get current state
	err := fwhelpers.UnpackModel(req.StateContents, &state)
	if err != nil {
		return schema.ErrorResponse(err)
	}

	res := schema.ServiceResponse{}

	if req.StateID != "" {
		// Query using existing previous state.
		hypertable, err := r.Client.ReadHypertable(req.StateID)
		if err != nil {
			return schema.ErrorResponse(err)
		}

		if hypertable.Deleted {
			// Hypertable does not exist.
			res.StateID = ""
			res.StateLastUpdated = ""
		} else {
			// Update state with refreshed value
			state.Id = hypertable.Id
			state.Name = hypertable.Name
			state.Description = hypertable.Description
			state.Tags = hypertable.Tags
			state.Admins = hypertable.Admins
			state.ShortName = hypertable.ShortName
			state.RefreshMode = hypertable.RefreshMode
			state.SqlSelect = hypertable.SqlSelect

			t := strings.Split(hypertable.LastModifiedDate, ".")[0] + "Z"
			tp, err := time.Parse(time.RFC3339, t)
			if err != nil {
				return schema.ErrorResponse(err)
			}

			res.StateID = hypertable.Id
			res.StateLastUpdated = tp.Format(time.RFC850)
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
func (r *hypertableResource) Update(req *schema.ServiceRequest) *schema.ServiceResponse {
	// logger := fwhelpers.GetLogger()

	// Retrieve values from plan
	var plan hypertableResourceModel
	err := fwhelpers.UnpackModel(req.PlanContents, &plan)
	if err != nil {
		return schema.ErrorResponse(err)
	}

	body := api.Hypertable{}
	body.Name = plan.Name
	body.Description = plan.Description
	body.Tags = plan.Tags
	body.Admins = plan.Admins
	body.ShortName = plan.ShortName
	body.RefreshMode = plan.RefreshMode
	body.SqlSelect = plan.SqlSelect

	// Update existing source
	_, err = r.Client.UpdateHypertable(plan.Id, body)
	if err != nil {
		return schema.ErrorResponse(err)
	}

	// Fetch updated items
	hypertable, err := r.Client.ReadHypertable(req.PlanID)
	if err != nil {
		return schema.ErrorResponse(err)
	}

	// Update state with refreshed value
	state := hypertableResourceModel{}
	state.Id = hypertable.Id
	state.Name = hypertable.Name
	state.Description = hypertable.Description
	state.Tags = hypertable.Tags
	state.Admins = hypertable.Admins
	state.ShortName = hypertable.ShortName
	state.RefreshMode = hypertable.RefreshMode
	state.SqlSelect = hypertable.SqlSelect

	// Set refreshed state
	stateEnc, err := fwhelpers.PackModel(nil, &state)
	if err != nil {
		return schema.ErrorResponse(err)
	}

	t := strings.Split(hypertable.LastModifiedDate, ".")[0] + "Z"
	tp, err := time.Parse(time.RFC3339, t)
	if err != nil {
		return schema.ErrorResponse(err)
	}

	return &schema.ServiceResponse{
		StateID:          hypertable.Id,
		StateContents:    stateEnc,
		StateLastUpdated: tp.Format(time.RFC850),
	}
}

// Delete deletes the resource and removes the state on success.
func (r *hypertableResource) Delete(req *schema.ServiceRequest) *schema.ServiceResponse {
	// Delete existing source
	err := r.Client.DeleteHypertable(req.StateID)
	if err != nil {
		return schema.ErrorResponse(err)
	}

	return &schema.ServiceResponse{}
}
