using KubeClient;
using KubeClient.Extensions.CustomResources;
using KubeClient.Models;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Reactive.Disposables;

namespace KubeKit
{
    /// <summary>
    ///     The base class for custom resource controllers.
    /// </summary>
    public abstract class CustomResourceController
        : IDisposable
    {
        readonly Dictionary<Type, Action<KubeResourceV1>> _createHandlers = new Dictionary<Type, Action<KubeResourceV1>>();
        readonly Dictionary<Type, Action<KubeResourceV1>> _modifyHandlers = new Dictionary<Type, Action<KubeResourceV1>>();
        readonly Dictionary<Type, Action<KubeResourceV1>> _deleteHandlers = new Dictionary<Type, Action<KubeResourceV1>>();
        
        readonly CompositeDisposable _watchSubscriptions = new CompositeDisposable();

        protected CustomResourceController(KubeApiClient kubeClient, ILogger logger)
        {
            if (kubeClient == null)
                throw new ArgumentNullException(nameof(kubeClient));

            if (logger == null)
                throw new ArgumentNullException(nameof(logger));
            
            Log = logger;
            KubeClient = kubeClient;
        }

        ~CustomResourceController() => Dispose(false);

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing) => _watchSubscriptions.Dispose();

        public bool IsRunning { get; private set; }

        protected KubeApiClient KubeClient { get; }

        protected ILogger Log { get; }

        protected void HandleCreate<TResource>(Action<TResource> handler)
            where TResource : KubeResourceV1
        {
            _createHandlers[typeof(TResource)] = resource =>
            {
                handler(
                    (TResource)resource
                );
            };
        }

        protected void HandleModify<TResource>(Action<TResource> handler)
            where TResource : KubeResourceV1
        {
            _modifyHandlers[typeof(TResource)] = resource =>
            {
                handler(
                    (TResource)resource
                );
            };
        }

        protected void HandleDelete<TResource>(Action<TResource> handler)
            where TResource : KubeResourceV1
        {
            _deleteHandlers[typeof(TResource)] = resource =>
            {
                handler(
                    (TResource)resource
                );
            };
        }

        protected abstract IEnumerable<IObservable<IResourceEventV1<KubeResourceV1>>> ResourceWatchers { get; }

        public void Start()
        {
            if (IsRunning)
                throw new InvalidOperationException($"{GetType().Name} is already running.");

            foreach (IObservable<IResourceEventV1<KubeResourceV1>> resourceWatcher in ResourceWatchers)
            {
                _watchSubscriptions.Add(resourceWatcher.Subscribe(
                    resourceEvent =>
                    {
                        switch (resourceEvent.EventType)
                        {
                            case ResourceEventType.Added:
                            {
                                OnResourceCreated(resourceEvent.Resource);

                                break;
                            }
                            case ResourceEventType.Modified:
                            {
                                OnResourceModified(resourceEvent.Resource);

                                break;
                            }
                            case ResourceEventType.Deleted:
                            {
                                OnResourceDeleted(resourceEvent.Resource);

                                break;
                            }
                            default:
                            {
                                Log.LogWarning("Unexpected event type '{EventType}' for '{ResourceType}' resource '{ResourceName}' in namespace '{ResourceNamespace}'.",
                                    resourceEvent.EventType,
                                    resourceEvent.Resource?.GetType()?.Name,
                                    resourceEvent.Resource?.Metadata?.Name,
                                    resourceEvent.Resource?.Metadata?.Namespace
                                );

                                break;
                            }
                        }
                    },
                    error => Log.LogError(error, "Error in resource-watch event stream.")
                ));
            }

            IsRunning = true;
        }

        public void Stop()
        {
            if (!IsRunning)
                throw new InvalidOperationException($"{GetType().Name} is not running.");

            foreach (IDisposable watchSubscription in _watchSubscriptions)
                watchSubscription.Dispose();

            _watchSubscriptions.Clear();

            IsRunning = false;
        }

        void OnResourceCreated(KubeResourceV1 resource)
        {
            if (resource == null)
                throw new ArgumentNullException(nameof(resource));
            
            Action<KubeResourceV1> handler;
            
            Type resourceType = resource.GetType();
            if (!_createHandlers.TryGetValue(resourceType, out handler))
            {
                Log.LogWarning("No creation event handler registered for '{ResourceType}' resources.", resourceType.FullName);

                return;
            }

            try
            {
                handler(resource);
            }
            catch (Exception handlerError)
            {
                Log.LogError(handlerError, "Unexpected error encountered by creation handler for '{HandlerType}' resource events.", resourceType.FullName);
            }
        }

        void OnResourceModified(KubeResourceV1 resource)
        {
            if (resource == null)
                throw new ArgumentNullException(nameof(resource));
            
            Action<KubeResourceV1> handler;
            
            Type resourceType = resource.GetType();
            if (!_modifyHandlers.TryGetValue(resourceType, out handler))
            {
                Log.LogWarning("No modification event handler registered for '{ResourceType}' resources.", resourceType.FullName);

                return;
            }

            try
            {
                handler(resource);
            }
            catch (Exception handlerError)
            {
                Log.LogError(handlerError, "Unexpected error encountered by modification event handler for '{HandlerType}' resource events.", resourceType.FullName);
            }
        }

        void OnResourceDeleted(KubeResourceV1 resource)
        {
            if (resource == null)
                throw new ArgumentNullException(nameof(resource));
            
            Action<KubeResourceV1> handler;
            
            Type resourceType = resource.GetType();
            if (!_deleteHandlers.TryGetValue(resourceType, out handler))
            {
                Log.LogWarning("No deletion event handler registered for '{ResourceType}' resources.", resourceType.FullName);

                return;
            }

            try
            {
                handler(resource);
            }
            catch (Exception handlerError)
            {
                Log.LogError(handlerError, "Unexpected error encountered by deletion event handler for '{HandlerType}' resource events.", resourceType.FullName);
            }
        }
    }
}