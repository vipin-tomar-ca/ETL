using AutoMapper;
using ETL.Enterprise.Application.DTOs;
using ETL.Enterprise.Domain.Entities;
using ETL.Enterprise.Domain.Enums;

namespace ETL.Enterprise.Application.Mapping;

/// <summary>
/// AutoMapper profile for ETL entities and DTOs
/// </summary>
public class ETLMappingProfile : Profile
{
    public ETLMappingProfile()
    {
        // ETLJob mappings
        CreateMap<ETLJob, ETLJobDto>()
            .ForMember(dest => dest.Configuration, opt => opt.MapFrom(src => src.Configuration))
            .ForMember(dest => dest.Logs, opt => opt.MapFrom(src => src.Logs));

        CreateMap<ETLJobDto, ETLJob>()
            .ForMember(dest => dest.Configuration, opt => opt.MapFrom(src => src.Configuration))
            .ForMember(dest => dest.Logs, opt => opt.MapFrom(src => src.Logs));

        // ETLJobConfiguration mappings
        CreateMap<ETLJobConfiguration, ETLJobConfigurationDto>()
            .ForMember(dest => dest.Source, opt => opt.MapFrom(src => src.Source))
            .ForMember(dest => dest.Target, opt => opt.MapFrom(src => src.Target))
            .ForMember(dest => dest.Transformation, opt => opt.MapFrom(src => src.Transformation))
            .ForMember(dest => dest.Scheduling, opt => opt.MapFrom(src => src.Scheduling))
            .ForMember(dest => dest.ErrorHandling, opt => opt.MapFrom(src => src.ErrorHandling))
            .ForMember(dest => dest.Performance, opt => opt.MapFrom(src => src.Performance))
            .ForMember(dest => dest.Engine, opt => opt.MapFrom(src => src.Engine));

        CreateMap<ETLJobConfigurationDto, ETLJobConfiguration>()
            .ForMember(dest => dest.Source, opt => opt.MapFrom(src => src.Source))
            .ForMember(dest => dest.Target, opt => opt.MapFrom(src => src.Target))
            .ForMember(dest => dest.Transformation, opt => opt.MapFrom(src => src.Transformation))
            .ForMember(dest => dest.Scheduling, opt => opt.MapFrom(src => src.Scheduling))
            .ForMember(dest => dest.ErrorHandling, opt => opt.MapFrom(src => src.ErrorHandling))
            .ForMember(dest => dest.Performance, opt => opt.MapFrom(src => src.Performance))
            .ForMember(dest => dest.Engine, opt => opt.MapFrom(src => src.Engine));

        // DataSourceConfiguration mappings
        CreateMap<DataSourceConfiguration, DataSourceConfigurationDto>().ReverseMap();

        // DataTargetConfiguration mappings
        CreateMap<DataTargetConfiguration, DataTargetConfigurationDto>().ReverseMap();

        // TransformationConfiguration mappings
        CreateMap<TransformationConfiguration, TransformationConfigurationDto>()
            .ForMember(dest => dest.Rules, opt => opt.MapFrom(src => src.Rules))
            .ForMember(dest => dest.Validation, opt => opt.MapFrom(src => src.Validation))
            .ForMember(dest => dest.Cleansing, opt => opt.MapFrom(src => src.Cleansing));

        CreateMap<TransformationConfigurationDto, TransformationConfiguration>()
            .ForMember(dest => dest.Rules, opt => opt.MapFrom(src => src.Rules))
            .ForMember(dest => dest.Validation, opt => opt.MapFrom(src => src.Validation))
            .ForMember(dest => dest.Cleansing, opt => opt.MapFrom(src => src.Cleansing));

        // TransformationRule mappings
        CreateMap<TransformationRule, TransformationRuleDto>().ReverseMap();

        // SchedulingConfiguration mappings
        CreateMap<SchedulingConfiguration, SchedulingConfigurationDto>()
            .ForMember(dest => dest.TimeZoneId, opt => opt.MapFrom(src => src.TimeZone != null ? src.TimeZone.Id : null));

        CreateMap<SchedulingConfigurationDto, SchedulingConfiguration>()
            .ForMember(dest => dest.TimeZone, opt => opt.MapFrom(src => 
                !string.IsNullOrEmpty(src.TimeZoneId) ? TimeZoneInfo.FindSystemTimeZoneById(src.TimeZoneId) : null));

        // ErrorHandlingConfiguration mappings
        CreateMap<ErrorHandlingConfiguration, ErrorHandlingConfigurationDto>().ReverseMap();

        // PerformanceConfiguration mappings
        CreateMap<PerformanceConfiguration, PerformanceConfigurationDto>().ReverseMap();

        // DataValidationConfiguration mappings
        CreateMap<DataValidationConfiguration, DataValidationConfigurationDto>()
            .ForMember(dest => dest.Rules, opt => opt.MapFrom(src => src.Rules));

        CreateMap<DataValidationConfigurationDto, DataValidationConfiguration>()
            .ForMember(dest => dest.Rules, opt => opt.MapFrom(src => src.Rules));

        // ValidationRule mappings
        CreateMap<ValidationRule, ValidationRuleDto>().ReverseMap();

        // DataCleansingConfiguration mappings
        CreateMap<DataCleansingConfiguration, DataCleansingConfigurationDto>().ReverseMap();

        // ETLJobLog mappings
        CreateMap<ETLJobLog, ETLJobLogDto>().ReverseMap();

        // ETLEngineConfiguration mappings
        CreateMap<ETLEngineConfiguration, ETLEngineConfigurationDto>()
            .ForMember(dest => dest.Resources, opt => opt.MapFrom(src => src.Resources));

        CreateMap<ETLEngineConfigurationDto, ETLEngineConfiguration>()
            .ForMember(dest => dest.Resources, opt => opt.MapFrom(src => src.Resources));

        // ResourceAllocation mappings
        CreateMap<ResourceAllocation, ResourceAllocationDto>().ReverseMap();
    }
}
