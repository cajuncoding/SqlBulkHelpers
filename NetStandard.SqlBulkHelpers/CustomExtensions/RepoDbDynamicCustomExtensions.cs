using Fasterflect;
using SqlBulkHelpers.Interfaces;
using SqlBulkHelpers.SqlBulkHelpers.CustomExtensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;

namespace SqlBulkHelpers.CustomExtensions
{
    internal static class RepoDbDynamicCustomExtensions
    {
        public static ISqlBulkHelpersPropertyTransformer FindRepoDbPropertyHandler(this PropertyInfo propInfo)
        {
            if (propInfo == null)
                return null;

            // 2) Detect RepoDb's [PropertyHandler(typeof(...))] by name (no RepoDb reference required)
            var repoDbAttr = propInfo
                .FindAttributesByName(RepoDbNames.PropertyHandlerAttributeName)
                .FirstOrDefault();

            if (repoDbAttr == null)
                return null;

            // 3) Extract the handler type from the attribute  with last ditch effort fallback if name/property differs
            var repoDbHandlerType = repoDbAttr.GetPropertyValue(RepoDbNames.PropertyHandlerTypePropertyName) as Type
                ?? repoDbAttr.GetType().Properties().FirstOrDefault(p => p.CanRead && p.PropertyType == TypeCache.Type)?.GetValue(repoDbAttr) as Type;

            if (repoDbHandlerType == null)
                return null;

            // 4) Locate the 'Set' method: TInput Set(TResult input, PropertyHandlerSetOptions options);
            var repoDbHandlerSetMethod = repoDbHandlerType
                .Methods()
                .FirstOrDefault(m =>
                    m.Name.Equals(RepoDbNames.PropertyHandlerSetMethodName, StringComparison.OrdinalIgnoreCase)
                    && !m.IsGenericMethodDefinition
                    && m.Parameters() is IList<ParameterInfo> methodParams
                    && methodParams.Count == 2 //Must have exactly 2 params (per RepoDb Interface) and validate the Name of the second param below...
                    && methodParams[0].ParameterType.IsAssignableFrom(propInfo.PropertyType) //Input Type (to be converted) must match the current Property Type we are processing!
                    && methodParams[1].ParameterType is Type methodType
                    //The Set method is used to write value to the DB!
                    && (methodType.FullName ?? methodType.Name).IndexOf(RepoDbNames.PropertyHandlerSetOptionsClassName, StringComparison.OrdinalIgnoreCase) >= 0
                );

            if (repoDbHandlerSetMethod == null)
                return null;

            try
            {
                // 5) Create an instance of the PropertyHandler class (as only the Type is reference in the Attribute)
                object propertyHandlerInstance = repoDbHandlerType.CreateInstance(); // Fasterflect; falls back to Activator if needed

                // 6) Create high‑performance Fasterflect invoker (delegate)
                var repoDbPropHandlerGetMethodInvoker = repoDbHandlerSetMethod.DelegateForCallMethod(); // returns Fasterflect.MethodInvoker

                // For safety, ensure the first arg (TInput) matches/accepts the property type
                //NOTE: We already know we have 2 parameters above!
                var parameters = repoDbHandlerSetMethod.Parameters();
                var optionsParamType = parameters[1].ParameterType;

                // Options: null for class, default(T) for struct which is returned when we try to create an instance...
                object optionsArg = optionsParamType.IsValueType
                    ? optionsParamType.CreateInstance()
                    : null;

                // 8) Return a converter wrapper Interfaced Func<object, object> to encapsulate the call to the Invoker
                //      of the Get mehod of the PropertyHandler
                return new SqlBulkLambdaPropertyConverter(valueObj =>
                {
                    if (valueObj is null) return null;
                    return repoDbPropHandlerGetMethodInvoker.Invoke(propertyHandlerInstance, valueObj, optionsArg);
                });
            }
            catch
            {
                return null; // could not construct; ignore optional feature
            }
        }
    }
}
