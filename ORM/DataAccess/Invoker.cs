using System;
using System.Linq.Expressions;
using System.Reflection;

namespace ORM.DataAccess
{
    public class Invoker
    {
        public static Func<T, TReturn> BuildTypedGetter<T, TReturn>(PropertyInfo propertyInfo)
        {
            var reflGet =
                (Func<T, TReturn>)Delegate.CreateDelegate(typeof(Func<T, TReturn>), propertyInfo.GetGetMethod());

            return reflGet;
        }

        public static Action<T, TProperty> BuildTypedSetter<T, TProperty>(PropertyInfo propertyInfo)
        {
            var reflSet =
                (Action<T, TProperty>)
                    Delegate.CreateDelegate(typeof(Action<T, TProperty>), propertyInfo.GetSetMethod());

            return reflSet;
        }

        public static Action<T, object> CreateSetter<T>(PropertyInfo propertyInfo)
        {
            var targetType = propertyInfo.DeclaringType;

            var info = propertyInfo.GetSetMethod();

            var type = propertyInfo.PropertyType;

            var target = Expression.Parameter(targetType, "t");

            var value = Expression.Parameter(typeof(object), "st");

            var condition = Expression.Condition(
                // test
                Expression.Equal(value, Expression.Constant(DBNull.Value)),
                // if true
                Expression.Default(type),
                // if false
                Expression.Convert(value, type)
                );

            var body = Expression.Call(
                Expression.Convert(target, info.DeclaringType),
                info,
                condition
                );

            var lambda = Expression.Lambda<Action<T, object>>(body, target, value);

            var action = lambda.Compile();

            return action;
        }

        public static Func<T, object> CreateGetter<T>(PropertyInfo propertyInfo)
        {
            var targetType = propertyInfo.DeclaringType;

            var info = propertyInfo.GetGetMethod();

            //var type = info.ReturnType;

            var exTarget = Expression.Parameter(targetType, "t");
            var exBody = Expression.Call(exTarget, info);
            var exBody2 = Expression.Convert(exBody, typeof(object));
            var lambda = Expression.Lambda<Func<T, object>>(exBody2, exTarget);

            var action = lambda.Compile();

            return action;
        }
    }
}