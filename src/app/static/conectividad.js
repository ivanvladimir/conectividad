// Initialize authorization
function initializeAuth() {
    // Check if tokens already exist
    const accessToken = localStorage.getItem('access_token');
    
    if (accessToken !== null && accessToken !== 'null'){
      if(verifyToken(accessToken)){
        Alpine.store('elements_open').data_open = true;
        Alpine.store('elements_open').login_open = false;
        Alpine.store('elements_open').loading_open = false;
        return true;
      }
    }
    Alpine.store('elements_open').login_open = true;
    Alpine.store('elements_open').data_open = false;
    Alpine.store('elements_open').loading_open = false;
    logout();
}

async function verifyToken(token) {
    try {
        const response = await fetch(window.app.urls['verify'], {
            method: 'POST',
            headers: { 
              'Authorization': `Bearer ${token}`,
              'Content-Type': 'application/json' 
            },
            credentials: 'include'
        });
        if (!response.ok) {
            // Token invalid, try to refresh
            return false;
        } else {
            return true;
        }
    } catch (error) {
        console.error('Token verification failed:', error);
    }
}

// Call on every page load
document.addEventListener('DOMContentLoaded', initializeAuth);


document.body.addEventListener('htmx:configRequest', function(event) {
    event.detail.headers['Authorization'] = 'Bearer ' + localStorage.access_token;
});

// Handle 401 responses - attempt token refresh
document.body.addEventListener('htmx:responseError', function(event) {
    if (event.detail.xhr.status === 401) {
        // Stop the current request
        event.stopPropagation();
        
        // Attempt to refresh the token
        refreshAccessToken().then(success => {
            if (success) {
                // Retry the original request
                const originalRequest = event.detail.requestConfig;
                htmx.ajax('POST', originalRequest.path, {
                    target: originalRequest.target,
                    swap: originalRequest.swap
                });
            } else {
                // If refresh fails, redirect to login
                logout();
            }
        });
    }
});

// Function to refresh access token
async function refreshAccessToken() {
    try {
        const response = await fetch(window.app.urls['refresh'], {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            credentials: 'include'
        });
        
        if (response.ok) {
            const data = await response.json();
            localStorage.access_token = data.access_token;
            localStorage.accessTokenExpiry = new Date().getTime() + (data.expiresIn * 1000);
            return true;
        } else {
            return false;
        }
    } catch (error) {
        console.error('Error refreshing token:', error);
        return false;
    }
}

// Handle login response
document.getElementById('login-message').addEventListener('htmx:afterSwap', function(event) {
  if (event.target.id === 'login-message') {
    try {
        const response = JSON.parse(event.detail.xhr.responseText);
        if (response.access_token) {
            // Store tokens
            localStorage.access_token = response.access_token;
            localStorage.accessTokenExpiry = new Date().getTime() + (response.expiresIn * 1000);
            
            // Update UI to show protected content
            Alpine.store('elements_open').login_open = false;
            Alpine.store('elements_open').data_open = true;
            
            // Clear the response area
            event.detail.innerHTML = '';
            
            // Load initial protected data
            document.getElementById('load-data-btn').click();
        }
    } catch (e) {
        // Not JSON, so it's probably an error message, leave it displayed
    }}
});

// Logout
function logout() {
    // Optionally notify server of logout
    fetch(window.app.urls['logout'], {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${localStorage.access_token}`,
          'Content-Type': 'application/json'
        },
        credentials: 'include'
    }).catch(err => console.error('Logout error:', err));
    // Clear tokens
    localStorage.access_token = null;
    localStorage.accessTokenExpiry = null;
    
    // Update UI
    Alpine.store('elements_open').login_open = true;
    Alpine.store('elements_open').data_open = false;
    document.getElementById('login-message').innerHTML = '';
}


document.addEventListener('alpine:init', () => {
    Alpine.store('elements_open', {
        init() {},

        login_open:false,
        data_open:false,
        loading_open:true,

        toggle(s) {
          if(s.localeCompare("login")){
            this.login_open = ! this.login_open
          }else{
          if(s.localeCompare("data")){
            this.data_open = ! this.data_open
          }else{
          if(s.localeCompare("loading")){
            this.loading_open = ! this.loading_open
          }}}
        }
    })
});

  function buttonComponent() {
      return {
          open1:false,
          open2:false,
          normalized:false,
          text:"",
          selectedVar1: "",
          selectedVar2: "",
          selectedVar1Label: "1a var.",
          selectedVar2Label: "2a var. opcional",
          variables: [
            {'id':'grade', 'label':'Grado'},
            {'id':'school_type', 'label':'Tipo de escuela'},
            {'id':'text_type2', 'label':'Tipo de texto'},
            {'id':'sex', 'label':'Sexo'},
          ],
          handleClick(target) {
              if(this.selectedVar1.length>0) {
              // Get current URL parameters
              const currentParams = new URLSearchParams(window.location.search);

              // Create the new URL with current parameters
              currentParams.set('var1', this.selectedVar1);
              if(this.selectedVar2) {
                currentParams.set('var2', this.selectedVar2);
              }
              if(this.text) {
                currentParams.set('text', this.text);
              }
              if(this.normalized){
                currentParams.set('normalized', true);
              }
              console.log(currentParams.toString());
              const newUrl = `${window.APP_CONFIG.baseUrl}graph/${target}?${currentParams.toString()}`;

              // Navigate to the new URL
              window.location.href = newUrl;
              }
          },
          toggle(variable) {
            if(variable === 1) {
              this.open1 = !this.open1;
            }else{
              this.open2 = !this.open2;
            }
          },
          close(variable) {
            if(variable === 1) {
              this.open1 = false;
            }else{
              this.open2 = false;
            }
          },
          selectItem(item, variable) {
            if(variable === 1) {
              this.selectedVar1 = item.id;
              this.selectedVar1Label = item.label;
            }else{
              this.selectedVar2 = item.id;
              this.selectedVar2Label = item.label;
            }
            this.close(variable);
          },
          isSelected(item,variable) {
            if(variable === 1){
              return this.selectedVar1 === item.id
            }else{
              return this.selectedVar2 === item.id
            }
          }
      }
  }



  function imageViewer() {
      return {
        imgUrl: null,
        error: null,
        access_token: null,
        tokenExpire: 0,
        dynamicImageUrl: '',
        loading: false,
        error: '',
        modal: null,
        image: {id: '', number: 0, array:[]},
        init() {
          const modalEl = document.getElementById('modals-image')
          modalEl?.addEventListener('hidden.bs.modal', this.resetModal)
        },
        resetModal() {
            this.image.info.forEach(img => {
              URL.revokeObjectURL(img.blob);
            });
            this.image.info = [];
        },
      async makeAuthenticatedRequest(url) {

            const response = await fetch(url, {
              headers: {
                'Authorization': `Bearer ${localStorage.access_token}`,
                'Content-Type': 'application/json'
              },
              credentials: 'include'
              }).catch(err => console.error('Logout error:', err));
              if (response.status === 401) {
                  this.logout();
                  throw new Error('Session expired. Please login again.');
              }
              return response;
          },

      getUrls(image_id,max){
        let urls =[];
        for (let i=1; i<= max; i++){
          let url = `${window.app.baseUrl}api/v1/image/${image_id}/${i}`;
          urls.push({
            url:url,
            blob:this.fetchImageAsBlob(url)
          });
        }
        return urls;
      },

      loadImage(image_id,image_number,max) {
              this.loading = true;
              this.error = '';
              this.image.id = image_id;
              this.image.number = image_number;
              this.image.prev = Math.max(image_number -1,1);
              this.image.next = Math.min(max,image_number +1);
              this.image.max = max;
              this.image.info = this.getUrls(image_id,max);
              if(this.image.info.length === 0) {
                  return '';
              }else{
                this.image.info[this.image.number-1].blob.then(blob => { this.dynamicImageUrl = URL.createObjectURL(blob);});
                this.loading = false;
              }
      },
      reloadImage(image_number) {
              this.loading = true;
              if(this.image.info.length === 0) {
                  return '';
              }else{
                this.image.number = image_number;
                this.image.prev = Math.max(image_number -1,1);
                this.image.next = Math.min(this.image.max,image_number +1);
                this.image.info[this.image.number-1].blob.then(blob => { this.dynamicImageUrl = URL.createObjectURL(blob);});
                this.loading = false;
              }
      },


      async fetchImageAsBlob(url) {
              const response = await this.makeAuthenticatedRequest(url);
              if (!response.ok) {
                  throw new Error('Failed to load image');
              }
              return await response.blob();
      },
      handleImageError(type) {
              this.loading[type] = false;
              this.error[type] = 'Failed to load image. Please try again.';
      },
    }
  }


class App {
    constructor() {
        this.config = window.APP_CONFIG || {};
        this.urls = this.config.urls || {};
        this.baseUrl = this.config.baseUrl || {};
        
        
        if (Object.keys(this.urls).length === 0) {
            console.warn('No URL configuration found. Make sure APP_CONFIG is loaded before this script.');
        }
        
        this.init();
    }

    init() {
        this.setupEventListeners();
    }

    setupEventListeners() {
        // Example: Handle form submissions
        document.addEventListener('submit', (e) => {
            if (e.target.classList.contains('search-form')) {
                e.preventDefault();
                this.handleSearch(e.target);
            }
        });
    }

    // Helper method to build URLs
    buildURL(routeName, params = {}, queryParams = {}) {
        if (!this.urls[routeName]) {
            console.error(`URL not found: ${routeName}`);
            return '#';
        }

        return window.buildURL(this.urls[routeName], params, queryParams);
    }

    // Example method using URLs
    handleSearch(form) {
        const formData = new FormData(form);
        const query = formData.get('q');
        
        const searchUrl = this.buildURL('search', {}, { q: query });
        window.location.href = searchUrl;
    }

    // Navigate to different pages
    navigateTo(routeName, params = {}, queryParams = {}) {
        const url = this.buildURL(routeName, params, queryParams);
        window.location.href = url;
    }

    // Make AJAX requests to API endpoints
    async apiCall(routeName, params = {}, options = {}) {
        const url = this.buildURL(routeName, params);
        
        const defaultOptions = {
            headers: {
                'Content-Type': 'application/json',
            }
        };

        const finalOptions = { ...defaultOptions, ...options };
        
        try {
            const response = await fetch(url, finalOptions);
            return await response.json();
        } catch (error) {
            console.error('API call failed:', error);
            throw error;
        }
    }
}

// Initialize app when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    window.app = new App();
});



