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
type datasourceResource struct {
	Client *api.Client
}

type datasourceResourceModel struct {
	Id                        string   `pctsdk:"id"`
	Name                      string   `pctsdk:"name"`
	Description               string   `pctsdk:"description"`
	Tags                      []string `pctsdk:"tags"`
	Admins                    []string `pctsdk:"admins"`
	ShortName                 string   `pctsdk:"short_name"`
	ConnectionMetadata        string   `pctsdk:"connection_metadata"`
	DbConnector               string   `pctsdk:"db_connector"`
	DbSubConnector            string   `pctsdk:"db_sub_connector"`
	DbSubConnectorDisplayName string   `pctsdk:"db_sub_connector_display_name"`
}

// Ensure the implementation satisfies the expected interfaces.
var (
	_ schema.ResourceService = &datasourceResource{}
)

// Helper function to return a resource service instance.
func NewDatasourceResource() schema.ResourceService {
	return &datasourceResource{}
}

// Metadata returns the resource type name.
// It is always provider name + "_" + resource type name.
func (r *datasourceResource) Metadata(req *schema.ServiceRequest) *schema.ServiceResponse {
	return &schema.ServiceResponse{
		TypeName: req.TypeName + "_datasource",
	}
}

// Configure adds the provider configured client to the resource.
func (r *datasourceResource) Configure(req *schema.ServiceRequest) *schema.ServiceResponse {
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
func (r *datasourceResource) Schema() *schema.ServiceResponse {
	s := &schema.Schema{
		Description: "Datasource resource for Zipstack Cloud",
		Attributes: map[string]schema.Attribute{
			"id": &schema.StringAttribute{
				Description: "ID",
				Computed:    true,
			},
			"name": &schema.StringAttribute{
				Description: "Name",
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
				},
			},
			"admins": &schema.ListAttribute{
				Description: "Admins",
				Required:    true,
				Optional:    true,
				NestedAttribute: &schema.StringAttribute{
					Description: "Admin",
					Required:    true,
				},
			},
			"short_name": &schema.StringAttribute{
				Description: "Short Name",
				Required:    true,
			},
			"connection_metadata": &schema.StringAttribute{
				Description: "Connection Metadata",
				Required:    true,
				Sensitive:   true,
			},
			"db_connector": &schema.StringAttribute{
				Description: "DB Connector",
				Required:    true,
			},
			"db_sub_connector": &schema.StringAttribute{
				Description: "DB Sub Connector",
				Required:    true,
			},
			"db_sub_connector_display_name": &schema.StringAttribute{
				Description: "DB Sub Connector Display Name",
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
func (r *datasourceResource) Create(req *schema.ServiceRequest) *schema.ServiceResponse {
	// logger := fwhelpers.GetLogger()

	// Retrieve values from plan
	var plan datasourceResourceModel
	err := fwhelpers.UnpackModel(req.PlanContents, &plan)
	if err != nil {
		return schema.ErrorResponse(err)
	}

	// Generate API request body from plan
	body := api.Datasource{}
	body.Name = plan.Name
	body.Description = plan.Description
	body.Tags = plan.Tags
	body.Admins = plan.Admins
	body.ShortName = plan.ShortName
	body.ConnectionMetadata = plan.ConnectionMetadata
	body.DbConnector = plan.DbConnector
	body.DbSubConnector = plan.DbSubConnector
	body.DbSubConnectorDisplayName = plan.DbSubConnectorDisplayName

	// Create new source
	datasource, err := r.Client.CreateDatasource(body)
	if err != nil {
		return schema.ErrorResponse(err)
	}

	// Update resource state with response body
	state := datasourceResourceModel{}
	state.Id = datasource.Id
	state.Name = plan.Name
	state.Description = plan.Description
	state.Tags = plan.Tags
	state.Admins = plan.Admins
	state.ShortName = plan.ShortName
	state.ConnectionMetadata = plan.ConnectionMetadata
	state.DbConnector = plan.DbConnector
	state.DbSubConnector = plan.DbSubConnector
	state.DbSubConnectorDisplayName = plan.DbSubConnectorDisplayName

	// Set refreshed state
	stateEnc, err := fwhelpers.PackModel(nil, &state)
	if err != nil {
		return schema.ErrorResponse(err)
	}

	return &schema.ServiceResponse{
		StateID:          datasource.Id,
		StateContents:    stateEnc,
		StateLastUpdated: time.Now().Format(time.RFC850),
	}
}

// Read resource information
func (r *datasourceResource) Read(req *schema.ServiceRequest) *schema.ServiceResponse {
	// logger := fwhelpers.GetLogger()

	var state datasourceResourceModel

	// Get current state
	err := fwhelpers.UnpackModel(req.StateContents, &state)
	if err != nil {
		return schema.ErrorResponse(err)
	}

	res := schema.ServiceResponse{}

	if req.StateID != "" {
		// Query using existing previous state.
		datasource, err := r.Client.ReadDatasource(req.StateID)
		if err != nil {
			return schema.ErrorResponse(err)
		}

		if datasource.Deleted {
			// Datasource does not exist.
			res.StateID = ""
			res.StateLastUpdated = ""
		} else {
			// Update state with refreshed value
			state.Id = datasource.Id
			state.Name = datasource.Name
			state.Description = datasource.Description
			state.Tags = datasource.Tags
			state.Admins = datasource.Admins
			state.ShortName = datasource.ShortName
			state.ConnectionMetadata = datasource.ConnectionMetadata
			state.DbConnector = datasource.DbConnector
			state.DbSubConnector = datasource.DbSubConnector
			state.DbSubConnectorDisplayName = datasource.DbSubConnectorDisplayName

			t := strings.Split(datasource.LastModifiedDate, ".")[0] + "Z"
			tp, err := time.Parse(time.RFC3339, t)
			if err != nil {
				return schema.ErrorResponse(err)
			}

			res.StateID = datasource.Id
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

func (r *datasourceResource) Update(req *schema.ServiceRequest) *schema.ServiceResponse {
	// logger := fwhelpers.GetLogger()

	// Retrieve values from plan
	var plan datasourceResourceModel
	err := fwhelpers.UnpackModel(req.PlanContents, &plan)
	if err != nil {
		return schema.ErrorResponse(err)
	}

	body := api.Datasource{}
	body.Name = plan.Name
	body.Description = plan.Description
	body.Tags = plan.Tags
	body.Admins = plan.Admins
	body.ShortName = plan.ShortName
	body.ConnectionMetadata = plan.ConnectionMetadata
	body.DbConnector = plan.DbConnector
	body.DbSubConnector = plan.DbSubConnector
	body.DbSubConnectorDisplayName = plan.DbSubConnectorDisplayName

	// Update existing source
	_, err = r.Client.UpdateDatasource(plan.Id, body)
	if err != nil {
		return schema.ErrorResponse(err)
	}

	// Fetch updated items
	datasource, err := r.Client.ReadDatasource(req.PlanID)
	if err != nil {
		return schema.ErrorResponse(err)
	}

	// Update state with refreshed value
	state := datasourceResourceModel{}
	state.Id = datasource.Id
	state.Name = datasource.Name
	state.Description = datasource.Description
	state.Tags = datasource.Tags
	state.Admins = datasource.Admins
	state.ShortName = datasource.ShortName
	state.ConnectionMetadata = datasource.ConnectionMetadata
	state.DbConnector = datasource.DbConnector
	state.DbSubConnector = datasource.DbSubConnector
	state.DbSubConnectorDisplayName = datasource.DbSubConnectorDisplayName

	// Set refreshed state
	stateEnc, err := fwhelpers.PackModel(nil, &state)
	if err != nil {
		return schema.ErrorResponse(err)
	}

	t := strings.Split(datasource.LastModifiedDate, ".")[0] + "Z"
	tp, err := time.Parse(time.RFC3339, t)
	if err != nil {
		return schema.ErrorResponse(err)
	}

	return &schema.ServiceResponse{
		StateID:          datasource.Id,
		StateContents:    stateEnc,
		StateLastUpdated: tp.Format(time.RFC850),
	}
}

// Delete deletes the resource and removes the state on success.
func (r *datasourceResource) Delete(req *schema.ServiceRequest) *schema.ServiceResponse {
	// Delete existing source
	err := r.Client.DeleteDatasource(req.StateID)
	if err != nil {
		return schema.ErrorResponse(err)
	}

	return &schema.ServiceResponse{}
}
