import { startPwa } from './lib/pwa';
import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import './styles.css';
import { App } from './App';
import { createBrowserRouter, RouterProvider } from 'react-router-dom';

// A data router (not BrowserRouter) so pages can block navigation with useBlocker; App keeps its own <Routes>.
startPwa();
const router = createBrowserRouter([{ path: '*', element: <App /> }]);
createRoot(document.getElementById('root')!).render(<StrictMode><RouterProvider router={router} /></StrictMode>);
