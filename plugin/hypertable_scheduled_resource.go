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
type hypertableScheduledResource struct {
	Client *api.Client
}

type hypertableScheduledResourceModel struct {
	Id                     string                     `pctsdk:"id"`
	Name                   string                     `pctsdk:"name"`
	Description            string                     `pctsdk:"description"`
	ShortName              string                     `pctsdk:"short_name"`
	Tags                   []string                   `pctsdk:"tags"`
	Admins                 []string                   `pctsdk:"admins"`
	RefreshMode            string                     `pctsdk:"refresh_mode"`
	CronTiming             string                     `pctsdk:"cron_timing"`
	CronTimingString       string                     `pctsdk:"cron_timing_string"`
	Stages                 []hypertableScheduledStage `pctsdk:"stages"`
	BackingTable           string                     `pctsdk:"backing_table"`
	BackingTableUpdateMode string                     `pctsdk:"backing_table_update_mode"`
	PrimaryKeys            []string                   `pctsdk:"primary_keys"`
	PartitionKeys          []string                   `pctsdk:"partition_keys"`
	RESTEndpoint           string                     `pctsdk:"rest_endpoint"`
	Active                 bool                       `pctsdk:"active"`
}

type hypertableScheduledStage struct {
	ID          int64  `pctsdk:"id"`
	Query       string `pctsdk:"query"`
	Name        string `pctsdk:"name"`
	ShortName   string `pctsdk:"short_name"`
	Description string `pctsdk:"description"`
	RunStatus   string `pctsdk:"run_status"`
	StartTime   string `pctsdk:"start_time"`
	Duration    string `pctsdk:"duration"`
	Errors      int64  `pctsdk:"errors"`
}

// Ensure the implementation satisfies the expected interfaces.
var (
	_ schema.ResourceService = &hypertableScheduledResource{}
)

// Helper function to return a resource service instance.
func NewHypertableScheduledResource() schema.ResourceService {
	return &hypertableScheduledResource{}
}

// Metadata returns the resource type name.
// It is always provider name + "_" + resource type name.
func (r *hypertableScheduledResource) Metadata(req *schema.ServiceRequest) *schema.ServiceResponse {
	return &schema.ServiceResponse{
		TypeName: req.TypeName + "_hypertable_scheduled",
	}
}

// Configure adds the provider configured client to the resource.
func (r *hypertableScheduledResource) Configure(req *schema.ServiceRequest) *schema.ServiceResponse {
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
func (r *hypertableScheduledResource) Schema() *schema.ServiceResponse {
	s := &schema.Schema{
		Description: "Hypertable resource for Zipstack Cloud",
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
			"depends_on": &schema.FloatAttribute{
				Description: "Depends On",
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
			"refresh_mode": &schema.StringAttribute{
				Description: "Refresh Mode",
				Required:    true,
			},
			"cron_timing": &schema.StringAttribute{
				Description: "Cron Timing",
				Required:    true,
			},
			"cron_timing_string": &schema.StringAttribute{
				Description: "Cron Timing String",
				Required:    true,
			},
			"stages": &schema.ListAttribute{
				Description: "Stages",
				Required:    true,
				NestedAttribute: &schema.MapAttribute{
					Description: "Stage",
					Required:    true,
					Attributes: map[string]schema.Attribute{
						"id": &schema.IntAttribute{
							Description: "ID",
							Required:    true,
						},
						"query": &schema.StringAttribute{
							Description: "Query",
							Required:    true,
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
						"run_status": &schema.StringAttribute{
							Description: "Run Status",
							Required:    true,
						},
						"start_time": &schema.StringAttribute{
							Description: "Start Time",
							Required:    true,
						},
						"duration": &schema.StringAttribute{
							Description: "Duration",
							Required:    true,
						},
						"errors": &schema.IntAttribute{
							Description: "errors",
							Required:    true,
						},
					},
				},
			},
			"backing_table": &schema.StringAttribute{
				Description: "Backing Table",
				Required:    true,
			},
			"backing_table_update_mode": &schema.StringAttribute{
				Description: "Backing Table Update Mode",
				Required:    true,
			},
			"primary_keys": &schema.ListAttribute{
				Description: "Primary Keys",
				Required:    true,
				Optional:    true,
				NestedAttribute: &schema.StringAttribute{
					Description: "Primary Key",
					Required:    true,
				},
			},
			"partition_keys": &schema.ListAttribute{
				Description: "Partition Keys",
				Required:    true,
				Optional:    true,
				NestedAttribute: &schema.StringAttribute{
					Description: "Partition Key",
					Required:    true,
				},
			},
			"rest_endpoint": &schema.StringAttribute{
				Description: "REST Endpoint",
				Required:    true,
				Optional:    true,
			},
			"active": &schema.BoolAttribute{
				Description: "Active",
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
func (r *hypertableScheduledResource) Create(req *schema.ServiceRequest) *schema.ServiceResponse {
	// logger := fwhelpers.GetLogger()

	// Retrieve values from plan
	var plan hypertableScheduledResourceModel
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

	body.CronTiming = plan.CronTiming
	body.CronTimingString = plan.CronTimingString

	if plan.Stages != nil {
		body.Stages = []api.HypertableScheduledStage{}
		var id int64 = 1

		for _, ps := range plan.Stages {
			stage := api.HypertableScheduledStage{
				ID:          id,
				Query:       ps.Query,
				Name:        ps.Name,
				ShortName:   ps.ShortName,
				Description: ps.Description,
				RunStatus:   ps.RunStatus,
				StartTime:   ps.StartTime,
				Duration:    ps.Duration,
				Errors:      ps.Errors,
			}
			body.Stages = append(body.Stages, stage)
		}
	}

	body.BackingTable = plan.BackingTable
	body.BackingTableUpdateMode = plan.BackingTableUpdateMode
	body.PrimaryKeys = plan.PrimaryKeys
	body.PartitionKeys = plan.PartitionKeys

	body.RESTEndpoint = plan.RESTEndpoint

	// TODO
	// Update active status via separate API call.
	// body.Active = plan.Active

	// Create new source
	hypertable, err := r.Client.CreateHypertable(body)
	if err != nil {
		return schema.ErrorResponse(err)
	}

	// Update resource state with response body
	state := hypertableScheduledResourceModel{}
	state.Id = hypertable.Id
	state.Name = plan.Name
	state.Description = plan.Description
	state.ShortName = plan.ShortName
	state.Tags = plan.Tags
	state.Admins = plan.Admins
	state.RefreshMode = plan.RefreshMode

	state.CronTiming = plan.CronTiming
	state.CronTimingString = plan.CronTimingString

	if plan.Stages != nil {
		state.Stages = []hypertableScheduledStage{}
		var id int64 = 1

		for _, ps := range plan.Stages {
			stage := hypertableScheduledStage{
				ID:          id,
				Query:       ps.Query,
				Name:        ps.Name,
				ShortName:   ps.ShortName,
				Description: ps.Description,
				RunStatus:   ps.RunStatus,
				StartTime:   ps.StartTime,
				Duration:    ps.Duration,
				Errors:      ps.Errors,
			}
			state.Stages = append(state.Stages, stage)
		}
	}

	state.BackingTable = plan.BackingTable
	state.BackingTableUpdateMode = plan.BackingTableUpdateMode
	state.PrimaryKeys = plan.PrimaryKeys
	state.PartitionKeys = plan.PartitionKeys

	state.RESTEndpoint = plan.RESTEndpoint

	state.Active = plan.Active

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
func (r *hypertableScheduledResource) Read(req *schema.ServiceRequest) *schema.ServiceResponse {
	// logger := fwhelpers.GetLogger()

	var state hypertableScheduledResourceModel

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

			state.CronTiming = hypertable.CronTiming
			state.CronTimingString = hypertable.CronTimingString

			if hypertable.Stages != nil {
				state.Stages = []hypertableScheduledStage{}
				var id int64 = 1

				for _, ps := range hypertable.Stages {
					stage := hypertableScheduledStage{
						ID:          id,
						Query:       ps.Query,
						Name:        ps.Name,
						ShortName:   ps.ShortName,
						Description: ps.Description,
						RunStatus:   ps.RunStatus,
						StartTime:   ps.StartTime,
						Duration:    ps.Duration,
						Errors:      ps.Errors,
					}
					state.Stages = append(state.Stages, stage)
				}
			}

			state.BackingTable = hypertable.BackingTable
			state.BackingTableUpdateMode = hypertable.BackingTableUpdateMode
			state.PrimaryKeys = hypertable.PrimaryKeys
			state.PartitionKeys = hypertable.PartitionKeys

			state.RESTEndpoint = hypertable.RESTEndpoint

			// TODO
			// Update active status via separate API call.
			// state.Active = hypertable.Active

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
func (r *hypertableScheduledResource) Update(req *schema.ServiceRequest) *schema.ServiceResponse {
	// logger := fwhelpers.GetLogger()

	// Retrieve values from plan
	var plan hypertableScheduledResourceModel
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

	body.CronTiming = plan.CronTiming
	body.CronTimingString = plan.CronTimingString

	if plan.Stages != nil {
		body.Stages = []api.HypertableScheduledStage{}
		var id int64 = 1

		for _, ps := range plan.Stages {
			stage := api.HypertableScheduledStage{
				ID:          id,
				Query:       ps.Query,
				Name:        ps.Name,
				ShortName:   ps.ShortName,
				Description: ps.Description,
				RunStatus:   ps.RunStatus,
				StartTime:   ps.StartTime,
				Duration:    ps.Duration,
				Errors:      ps.Errors,
			}
			body.Stages = append(body.Stages, stage)
		}
	}

	body.BackingTable = plan.BackingTable
	body.BackingTableUpdateMode = plan.BackingTableUpdateMode
	body.PrimaryKeys = plan.PrimaryKeys
	body.PartitionKeys = plan.PartitionKeys

	body.RESTEndpoint = plan.RESTEndpoint

	// TODO
	// Update active status via separate API call.
	// body.Active = plan.Active

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
	state := hypertableScheduledResourceModel{}
	state.Id = hypertable.Id
	state.Name = hypertable.Name
	state.Description = hypertable.Description
	state.Tags = hypertable.Tags
	state.Admins = hypertable.Admins
	state.ShortName = hypertable.ShortName
	state.RefreshMode = hypertable.RefreshMode

	state.CronTiming = hypertable.CronTiming
	state.CronTimingString = hypertable.CronTimingString

	if hypertable.Stages != nil {
		state.Stages = []hypertableScheduledStage{}
		var id int64 = 1

		for _, ps := range hypertable.Stages {
			stage := hypertableScheduledStage{
				ID:          id,
				Query:       ps.Query,
				Name:        ps.Name,
				ShortName:   ps.ShortName,
				Description: ps.Description,
				RunStatus:   ps.RunStatus,
				StartTime:   ps.StartTime,
				Duration:    ps.Duration,
				Errors:      ps.Errors,
			}
			state.Stages = append(state.Stages, stage)
		}
	}

	state.BackingTable = hypertable.BackingTable
	state.BackingTableUpdateMode = hypertable.BackingTableUpdateMode
	state.PrimaryKeys = hypertable.PrimaryKeys
	state.PartitionKeys = hypertable.PartitionKeys

	state.RESTEndpoint = hypertable.RESTEndpoint

	// TODO
	// Update active status via separate API call.
	// state.Active = hypertable.Active

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
func (r *hypertableScheduledResource) Delete(req *schema.ServiceRequest) *schema.ServiceResponse {
	// Delete existing source
	err := r.Client.DeleteHypertable(req.StateID)
	if err != nil {
		return schema.ErrorResponse(err)
	}

	return &schema.ServiceResponse{}
}
